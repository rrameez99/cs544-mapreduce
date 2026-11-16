"""Boss process: the gRPC server that the client and the workers talk to.

Run with `python -m boss.server` from the repo root (the Docker image does this).

The RPC handlers here only translate between gRPC and the two pieces of boss
state: WorkerRegistry (boss/workers.py: who is alive) and Scheduler
(boss/jobs.py: jobs, tasks, attempts). All the decisions live in those two.
"""

import logging
import os
import sys
import threading
import time
from concurrent import futures

import grpc

from boss.events import JsonlEventLog
from boss.jobs import InvalidJobError, InvalidReportError, Scheduler, UnknownJobError, list_input_files
from boss.workers import UnknownWorkerError, WorkerRegistry
from common.env import require_int_env
from protos import mapreduce_pb2 as pb
from protos import mapreduce_pb2_grpc as pb_grpc

# Mounted into every container by docker-compose.yml. Job input and output
# paths must be inside it, because every worker has to be able to read and
# write them.
SHARED_ROOT = "/shared"
# Workers heartbeat every second (see worker/server.py), so 3 missed heartbeats mean dead.
HEARTBEAT_TIMEOUT_SECONDS = 3.0
LIVENESS_CHECK_INTERVAL_SECONDS = 1.0
GRPC_THREADS = 16

log = logging.getLogger("boss")


def resolve_shared_path(path: str, shared_root: str) -> str:
    """Return the absolute, symlink-free form of path, which must be inside shared_root."""
    resolved = os.path.realpath(path)
    root = os.path.realpath(shared_root)
    if os.path.commonpath([resolved, root]) != root:
        raise InvalidJobError(f"{path} is not inside shared storage ({shared_root})")
    return resolved


class BossServicer(pb_grpc.BossServicer):
    def __init__(self, registry: WorkerRegistry, scheduler: Scheduler, shared_root: str):
        self._registry = registry
        self._scheduler = scheduler
        self._shared_root = shared_root

    # ----- client -> boss -----

    def SubmitJob(self, request, context):
        spec = request.spec
        try:
            input_path = resolve_shared_path(spec.input_path, self._shared_root)
            output_path = resolve_shared_path(spec.output_path, self._shared_root)
            input_files = list_input_files(input_path)
            # Creating the output directory up front reserves it, so two jobs
            # can never write into the same directory. Reducers add their
            # part files to it.
            try:
                os.makedirs(output_path)
            except FileExistsError:
                raise InvalidJobError(f"output path {spec.output_path} already exists; choose a new one")
            try:
                job_id = self._scheduler.submit_job(spec, input_files)
            except InvalidJobError:
                os.rmdir(output_path)
                raise
        except InvalidJobError as error:
            context.abort(grpc.StatusCode.INVALID_ARGUMENT, str(error))
        return pb.SubmitJobResponse(job_id=job_id)

    def GetJobStatus(self, request, context):
        try:
            return self._scheduler.job_status(request.job_id)
        except UnknownJobError:
            context.abort(grpc.StatusCode.NOT_FOUND, f"no job with ID {request.job_id!r}")

    # ----- worker -> boss -----

    def Register(self, request, context):
        was_registered = self._registry.register(request.worker_id, request.fetch_address)
        if was_registered:
            # Same worker ID registering again means the container restarted
            # before we noticed it was gone: its running attempts and its map
            # output on local disk are lost. This is safe to do after
            # register(): the new process sends no heartbeat, so it gets no
            # task, until this RPC returns.
            log.info("worker %s re-registered at %s; treating it as restarted", request.worker_id, request.fetch_address)
            self._scheduler.handle_dead_worker(request.worker_id)
        else:
            log.info("worker %s registered at %s", request.worker_id, request.fetch_address)
        log.info("live workers: %s", ", ".join(self._registry.live_worker_ids()))
        return pb.RegisterResponse()

    def Heartbeat(self, request, context):
        try:
            fetch_address = self._registry.record_heartbeat(request.worker_id)
        except UnknownWorkerError:
            context.abort(
                grpc.StatusCode.NOT_FOUND,
                f"worker {request.worker_id!r} is not registered; register again",
            )
        if not request.ready_for_task:
            return pb.HeartbeatResponse()

        # If this response never reaches the worker, the attempt is still
        # recorded as running; the task timeout gives it to someone else.
        task = self._scheduler.assign_task(request.worker_id, fetch_address)
        if task is None:
            return pb.HeartbeatResponse()
        if isinstance(task, pb.MapTask):
            return pb.HeartbeatResponse(map_task=task)
        return pb.HeartbeatResponse(reduce_task=task)

    def ReportTaskDone(self, request, context):
        try:
            accepted = self._scheduler.report_task_done(request.worker_id, request.attempt, request.metrics)
        except UnknownJobError:
            context.abort(grpc.StatusCode.NOT_FOUND, f"no job with ID {request.attempt.job_id!r}")
        except InvalidReportError as error:
            context.abort(grpc.StatusCode.INVALID_ARGUMENT, str(error))
        return pb.ReportAck(accepted=accepted)

    def ReportTaskFailed(self, request, context):
        unfetchable_map_task_id = (
            request.unfetchable_map_task_id if request.HasField("unfetchable_map_task_id") else None
        )
        try:
            accepted = self._scheduler.report_task_failed(
                request.worker_id, request.attempt, request.error, unfetchable_map_task_id
            )
        except UnknownJobError:
            context.abort(grpc.StatusCode.NOT_FOUND, f"no job with ID {request.attempt.job_id!r}")
        except InvalidReportError as error:
            context.abort(grpc.StatusCode.INVALID_ARGUMENT, str(error))
        return pb.ReportAck(accepted=accepted)


def reschedule_work_of_dead_workers(registry: WorkerRegistry, scheduler: Scheduler) -> None:
    for worker_id in registry.remove_dead_workers():
        log.warning(
            "worker %s sent no heartbeat for over %.0fs; marking it dead",
            worker_id,
            HEARTBEAT_TIMEOUT_SECONDS,
        )
        scheduler.handle_dead_worker(worker_id)
        log.info("live workers: %s", ", ".join(registry.live_worker_ids()) or "(none)")


def check_liveness_forever(registry: WorkerRegistry, scheduler: Scheduler) -> None:
    while True:
        time.sleep(LIVENESS_CHECK_INTERVAL_SECONDS)
        reschedule_work_of_dead_workers(registry, scheduler)


def exit_on_thread_crash(args: threading.ExceptHookArgs) -> None:
    """A crashed background thread would leave the boss half-working, so stop the whole process instead."""
    log.critical(
        "thread %s crashed; exiting",
        args.thread.name if args.thread is not None else "?",
        exc_info=(args.exc_type, args.exc_value, args.exc_traceback),
    )
    os._exit(1)


def build_server(
    registry: WorkerRegistry, scheduler: Scheduler, shared_root: str, address: str
) -> tuple[grpc.Server, int]:
    """Create the boss gRPC server bound to address (host:port). Returns the server and the bound port."""
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=GRPC_THREADS))
    pb_grpc.add_BossServicer_to_server(BossServicer(registry, scheduler, shared_root), server)
    port = server.add_insecure_port(address)
    if port == 0:
        raise RuntimeError(f"could not bind the boss server to {address}")
    return server, port


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(levelname)s %(message)s")
    threading.excepthook = exit_on_thread_crash
    port = require_int_env("BOSS_PORT")
    # An attempt running longer than this gets a second attempt elsewhere
    # (design.md §5.1). docker-compose.yml sets it (30 s unless overridden).
    task_timeout_seconds = require_int_env("TASK_TIMEOUT_SECONDS")
    if task_timeout_seconds < 1:
        sys.exit(f"TASK_TIMEOUT_SECONDS must be at least 1, got {task_timeout_seconds}")

    registry = WorkerRegistry(HEARTBEAT_TIMEOUT_SECONDS, clock=time.monotonic)
    scheduler = Scheduler(task_timeout_seconds, clock=time.monotonic, events=JsonlEventLog())
    server, _ = build_server(registry, scheduler, SHARED_ROOT, f"0.0.0.0:{port}")
    server.start()
    log.info("boss listening on port %d (task timeout %ds)", port, task_timeout_seconds)

    threading.Thread(
        target=check_liveness_forever, args=(registry, scheduler), name="liveness", daemon=True
    ).start()
    server.wait_for_termination()


if __name__ == "__main__":
    main()
