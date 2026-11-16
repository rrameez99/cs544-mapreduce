"""Worker process: registers with the boss and sends heartbeats.

Run with `python -m worker.server` from the repo root (the Docker image does this).
"""

import logging
import os
import shutil
import sys
import threading
import time
from concurrent import futures
from pathlib import Path

import grpc

from common.env import require_env, require_int_env
from protos import mapreduce_pb2 as pb
from protos import mapreduce_pb2_grpc as pb_grpc
from worker.runner import TaskRunner, UnfetchableMapOutputError
from worker.storage import DEFAULT_LOCAL_ROOT, StorageError, map_partition_path_from_request

HEARTBEAT_INTERVAL_SECONDS = 1.0
RPC_TIMEOUT_SECONDS = 2.0
# How long to wait for the boss at startup, and how long the boss may stay
# unreachable afterwards before this worker gives up.
BOSS_UNREACHABLE_LIMIT_SECONDS = 30.0
GRPC_THREADS = 8
FETCH_CHUNK_BYTES = 1024 * 1024

log = logging.getLogger("worker")


class WorkerServicer(pb_grpc.WorkerServicer):
    """Serves accepted mapper partitions from this worker's local disk."""

    def __init__(self, local_root: Path = DEFAULT_LOCAL_ROOT):
        self._local_root = local_root

    def FetchPartition(self, request, context):
        try:
            partition_path = map_partition_path_from_request(
                self._local_root,
                request.job_id,
                request.map_task_id,
                request.attempt_id,
                request.reduce_task_id,
            )
        except StorageError as error:
            context.abort(grpc.StatusCode.INVALID_ARGUMENT, str(error))
        if not partition_path.is_file():
            context.abort(
                grpc.StatusCode.NOT_FOUND,
                f"partition for map task {request.map_task_id} attempt {request.attempt_id} is unavailable",
            )
        try:
            with partition_path.open("rb") as partition_file:
                while chunk := partition_file.read(FETCH_CHUNK_BYTES):
                    yield pb.PartitionChunk(data=chunk)
        except OSError as error:
            context.abort(
                grpc.StatusCode.INTERNAL,
                f"could not read partition for map task {request.map_task_id}: {error}",
            )


class AttemptExecutor:
    """Runs at most one assignment while the heartbeat loop remains responsive."""

    def __init__(self, boss: pb_grpc.BossStub, worker_id: str, runner: TaskRunner):
        self._boss = boss
        self._worker_id = worker_id
        self._runner = runner
        self._lock = threading.Lock()
        self._thread: threading.Thread | None = None

    @property
    def is_running(self) -> bool:
        with self._lock:
            return self._thread is not None and self._thread.is_alive()

    def start(self, assignment: pb.MapTask | pb.ReduceTask) -> None:
        with self._lock:
            if self._thread is not None and self._thread.is_alive():
                raise RuntimeError("worker received an assignment while an earlier attempt is still running")
            attempt = assignment.attempt
            thread = threading.Thread(
                target=self._execute,
                args=(assignment,),
                name=f"{attempt.job_id}-{attempt.task_id}.{attempt.attempt_id}",
                daemon=True,
            )
            self._thread = thread
            thread.start()

    def _execute(self, assignment: pb.MapTask | pb.ReduceTask) -> None:
        attempt = assignment.attempt
        try:
            if isinstance(assignment, pb.MapTask):
                result = self._runner.run_map(assignment)
            else:
                result = self._runner.run_reduce(assignment)
        except UnfetchableMapOutputError as error:
            self._report_failure(attempt, str(error), error.map_task_id)
        except Exception as error:
            log.exception("%s failed", self._attempt_description(attempt))
            self._report_failure(attempt, str(error), None)
        else:
            self._report_done(attempt, result.metrics)
        finally:
            with self._lock:
                self._thread = None

    def _report_done(self, attempt: pb.TaskAttempt, metrics: pb.TaskMetrics) -> None:
        try:
            acknowledgement = self._boss.ReportTaskDone(
                pb.TaskDoneReport(worker_id=self._worker_id, attempt=attempt, metrics=metrics),
                timeout=RPC_TIMEOUT_SECONDS,
            )
        except grpc.RpcError as error:
            log.error("could not report completion of %s: %s", self._attempt_description(attempt), error)
            return
        if acknowledgement.accepted:
            log.info("boss accepted completion of %s", self._attempt_description(attempt))
        else:
            log.info("boss ignored late completion of %s", self._attempt_description(attempt))

    def _report_failure(
        self, attempt: pb.TaskAttempt, error_message: str, unfetchable_map_task_id: int | None
    ) -> None:
        report = pb.TaskFailedReport(
            worker_id=self._worker_id,
            attempt=attempt,
            error=error_message,
        )
        if unfetchable_map_task_id is not None:
            report.unfetchable_map_task_id = unfetchable_map_task_id
        try:
            acknowledgement = self._boss.ReportTaskFailed(report, timeout=RPC_TIMEOUT_SECONDS)
        except grpc.RpcError as error:
            log.error("could not report failure of %s: %s", self._attempt_description(attempt), error)
            return
        if acknowledgement.accepted:
            log.warning("boss accepted failure of %s: %s", self._attempt_description(attempt), error_message)
        else:
            log.info("boss ignored late failure of %s", self._attempt_description(attempt))

    @staticmethod
    def _attempt_description(attempt: pb.TaskAttempt) -> str:
        kind = "map" if attempt.kind == pb.TASK_KIND_MAP else "reduce"
        return f"{attempt.job_id} {kind} task {attempt.task_id} attempt {attempt.attempt_id}"


def clear_local_data(local_root: Path) -> None:
    """Delete everything under local_root, keeping the directory itself.

    Map output on local disk is only useful while the boss still knows this
    worker. Once it registers again, the boss treats it as a brand-new worker
    (its old attempts and map outputs were already given to others), so
    nothing here can ever be fetched again. Leaving it would also collide with
    new work: job IDs start again at job-1 when the boss restarts, so a new
    job-1 attempt would find an old job-1 directory already there.
    """
    local_root.mkdir(parents=True, exist_ok=True)
    for entry in local_root.iterdir():
        if entry.is_dir() and not entry.is_symlink():
            shutil.rmtree(entry)
        else:
            entry.unlink()


def register(boss: pb_grpc.BossStub, worker_id: str, fetch_address: str, local_root: Path) -> None:
    # Clear before registering, so no task from the new registration can
    # start writing while old files are still being deleted.
    clear_local_data(local_root)
    boss.Register(
        pb.RegisterRequest(worker_id=worker_id, fetch_address=fetch_address),
        timeout=RPC_TIMEOUT_SECONDS,
    )
    log.info("registered with the boss as %s (fetch address %s)", worker_id, fetch_address)


def heartbeat_forever(
    boss: pb_grpc.BossStub,
    worker_id: str,
    fetch_address: str,
    local_root: Path,
    executor: AttemptExecutor,
) -> None:
    last_contact_time = time.monotonic()
    while True:
        try:
            response = boss.Heartbeat(
                pb.HeartbeatRequest(worker_id=worker_id, ready_for_task=not executor.is_running),
                timeout=RPC_TIMEOUT_SECONDS,
            )
        except grpc.RpcError as error:
            if error.code() == grpc.StatusCode.NOT_FOUND:
                # The boss declared us dead (or restarted). Start over as a new registration.
                log.warning("boss does not know this worker (%s); registering again", error.details())
                register(boss, worker_id, fetch_address, local_root)
                last_contact_time = time.monotonic()
            elif error.code() in (grpc.StatusCode.UNAVAILABLE, grpc.StatusCode.DEADLINE_EXCEEDED):
                unreachable_seconds = time.monotonic() - last_contact_time
                if unreachable_seconds > BOSS_UNREACHABLE_LIMIT_SECONDS:
                    sys.exit(f"boss unreachable for {unreachable_seconds:.0f}s; exiting")
                log.warning("heartbeat failed (%s); retrying", error.code().name)
            else:
                raise
        else:
            last_contact_time = time.monotonic()
            assignment = response.WhichOneof("assignment")
            if assignment is not None:
                if executor.is_running:
                    raise RuntimeError(f"boss sent a {assignment} while this worker is already busy")
                executor.start(response.map_task if assignment == "map_task" else response.reduce_task)
        time.sleep(HEARTBEAT_INTERVAL_SECONDS)


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(levelname)s %(message)s")
    worker_id = require_env("WORKER_ID")
    worker_port = require_int_env("WORKER_PORT")
    boss_address = require_env("BOSS_ADDRESS")
    # Other containers reach this worker by its Compose service name, which is its ID.
    fetch_address = f"{worker_id}:{worker_port}"
    local_root = Path(os.environ.get("LOCAL_ROOT", str(DEFAULT_LOCAL_ROOT)))
    shared_root = Path(os.environ.get("SHARED_ROOT", "/shared"))

    server = grpc.server(futures.ThreadPoolExecutor(max_workers=GRPC_THREADS))
    pb_grpc.add_WorkerServicer_to_server(WorkerServicer(local_root), server)
    server.add_insecure_port(f"0.0.0.0:{worker_port}")
    server.start()
    log.info("worker %s serving partitions on port %d", worker_id, worker_port)

    channel = grpc.insecure_channel(boss_address)
    try:
        grpc.channel_ready_future(channel).result(timeout=BOSS_UNREACHABLE_LIMIT_SECONDS)
    except grpc.FutureTimeoutError:
        sys.exit(f"could not reach the boss at {boss_address} within {BOSS_UNREACHABLE_LIMIT_SECONDS:.0f}s")
    boss = pb_grpc.BossStub(channel)

    register(boss, worker_id, fetch_address, local_root)
    runner = TaskRunner(local_root, shared_root)
    executor = AttemptExecutor(boss, worker_id, runner)
    heartbeat_forever(boss, worker_id, fetch_address, local_root, executor)


if __name__ == "__main__":
    main()
