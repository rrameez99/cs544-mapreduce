"""Worker process: registers with the boss and sends heartbeats.

Run with `python -m worker.server` from the repo root (the Docker image does this).
"""

import logging
import sys
import time
from concurrent import futures

import grpc

from common.env import require_env, require_int_env
from protos import mapreduce_pb2 as pb
from protos import mapreduce_pb2_grpc as pb_grpc

HEARTBEAT_INTERVAL_SECONDS = 1.0
RPC_TIMEOUT_SECONDS = 2.0
# How long to wait for the boss at startup, and how long the boss may stay
# unreachable afterwards before this worker gives up.
BOSS_UNREACHABLE_LIMIT_SECONDS = 30.0
GRPC_THREADS = 8

log = logging.getLogger("worker")


class WorkerServicer(pb_grpc.WorkerServicer):
    """FetchPartition is not overridden yet, so it returns UNIMPLEMENTED."""


def register(boss: pb_grpc.BossStub, worker_id: str, fetch_address: str) -> None:
    boss.Register(
        pb.RegisterRequest(worker_id=worker_id, fetch_address=fetch_address),
        timeout=RPC_TIMEOUT_SECONDS,
    )
    log.info("registered with the boss as %s (fetch address %s)", worker_id, fetch_address)


def heartbeat_forever(boss: pb_grpc.BossStub, worker_id: str, fetch_address: str) -> None:
    last_contact_time = time.monotonic()
    while True:
        try:
            response = boss.Heartbeat(
                pb.HeartbeatRequest(worker_id=worker_id, ready_for_task=True),
                timeout=RPC_TIMEOUT_SECONDS,
            )
        except grpc.RpcError as error:
            if error.code() == grpc.StatusCode.NOT_FOUND:
                # The boss declared us dead (or restarted). Start over as a new registration.
                log.warning("boss does not know this worker (%s); registering again", error.details())
                register(boss, worker_id, fetch_address)
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
                raise NotImplementedError(f"boss sent a {assignment}, but task execution is not implemented yet")
        time.sleep(HEARTBEAT_INTERVAL_SECONDS)


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(levelname)s %(message)s")
    worker_id = require_env("WORKER_ID")
    worker_port = require_int_env("WORKER_PORT")
    boss_address = require_env("BOSS_ADDRESS")
    # Other containers reach this worker by its Compose service name, which is its ID.
    fetch_address = f"{worker_id}:{worker_port}"

    server = grpc.server(futures.ThreadPoolExecutor(max_workers=GRPC_THREADS))
    pb_grpc.add_WorkerServicer_to_server(WorkerServicer(), server)
    server.add_insecure_port(f"0.0.0.0:{worker_port}")
    server.start()
    log.info("worker %s serving partitions on port %d", worker_id, worker_port)

    channel = grpc.insecure_channel(boss_address)
    try:
        grpc.channel_ready_future(channel).result(timeout=BOSS_UNREACHABLE_LIMIT_SECONDS)
    except grpc.FutureTimeoutError:
        sys.exit(f"could not reach the boss at {boss_address} within {BOSS_UNREACHABLE_LIMIT_SECONDS:.0f}s")
    boss = pb_grpc.BossStub(channel)

    register(boss, worker_id, fetch_address)
    heartbeat_forever(boss, worker_id, fetch_address)


if __name__ == "__main__":
    main()
