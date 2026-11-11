"""Boss process: the gRPC server that the client and the workers talk to.

Run with `python -m boss.server` from the repo root (the Docker image does this).
"""

import logging
import os
import threading
import time
from concurrent import futures

import grpc

from boss.workers import UnknownWorkerError, WorkerRegistry
from common.env import require_int_env
from protos import mapreduce_pb2 as pb
from protos import mapreduce_pb2_grpc as pb_grpc

# Workers heartbeat every second (see worker/server.py), so 3 missed heartbeats mean dead.
HEARTBEAT_TIMEOUT_SECONDS = 3.0
LIVENESS_CHECK_INTERVAL_SECONDS = 1.0
GRPC_THREADS = 16

log = logging.getLogger("boss")


class BossServicer(pb_grpc.BossServicer):
    """Handles worker registration and heartbeats.

    Job and task RPCs (SubmitJob, GetJobStatus, ReportTaskDone,
    ReportTaskFailed) are not overridden yet, so they return UNIMPLEMENTED.
    """

    def __init__(self, registry: WorkerRegistry):
        self._registry = registry

    def Register(self, request, context):
        was_registered = self._registry.register(request.worker_id, request.fetch_address)
        if was_registered:
            log.info("worker %s re-registered at %s", request.worker_id, request.fetch_address)
        else:
            log.info("worker %s registered at %s", request.worker_id, request.fetch_address)
        log.info("live workers: %s", ", ".join(self._registry.live_worker_ids()))
        return pb.RegisterResponse()

    def Heartbeat(self, request, context):
        try:
            self._registry.record_heartbeat(request.worker_id)
        except UnknownWorkerError:
            context.abort(
                grpc.StatusCode.NOT_FOUND,
                f"worker {request.worker_id!r} is not registered; register again",
            )
        # No tasks to hand out yet; task assignment comes with the scheduler.
        return pb.HeartbeatResponse()


def check_liveness_forever(registry: WorkerRegistry) -> None:
    while True:
        time.sleep(LIVENESS_CHECK_INTERVAL_SECONDS)
        for worker_id in registry.remove_dead_workers():
            log.warning(
                "worker %s sent no heartbeat for over %.0fs; marking it dead",
                worker_id,
                HEARTBEAT_TIMEOUT_SECONDS,
            )
            log.info("live workers: %s", ", ".join(registry.live_worker_ids()) or "(none)")


def exit_on_thread_crash(args: threading.ExceptHookArgs) -> None:
    """A crashed background thread would leave the boss half-working, so stop the whole process instead."""
    log.critical(
        "thread %s crashed; exiting",
        args.thread.name if args.thread is not None else "?",
        exc_info=(args.exc_type, args.exc_value, args.exc_traceback),
    )
    os._exit(1)


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(levelname)s %(message)s")
    threading.excepthook = exit_on_thread_crash
    port = require_int_env("BOSS_PORT")

    registry = WorkerRegistry(HEARTBEAT_TIMEOUT_SECONDS, clock=time.monotonic)
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=GRPC_THREADS))
    pb_grpc.add_BossServicer_to_server(BossServicer(registry), server)
    server.add_insecure_port(f"0.0.0.0:{port}")
    server.start()
    log.info("boss listening on port %d", port)

    threading.Thread(target=check_liveness_forever, args=(registry,), name="liveness", daemon=True).start()
    server.wait_for_termination()


if __name__ == "__main__":
    main()
