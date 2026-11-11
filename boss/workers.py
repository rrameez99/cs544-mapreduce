"""Tracks which workers are registered and whether they are still alive."""

import threading
from dataclasses import dataclass
from typing import Callable


class UnknownWorkerError(Exception):
    """A heartbeat came from a worker that is not registered.

    This happens if the worker never registered, or if the boss already
    declared it dead. Either way the worker must register again, and anything
    it was doing has been (or will be) given to other workers.
    """


@dataclass
class WorkerRecord:
    worker_id: str
    fetch_address: str
    last_heartbeat_time: float


class WorkerRegistry:
    """Thread-safe table of live workers.

    `clock` returns the current time in seconds; tests pass a fake clock.
    """

    def __init__(self, heartbeat_timeout_seconds: float, clock: Callable[[], float]):
        self._heartbeat_timeout_seconds = heartbeat_timeout_seconds
        self._clock = clock
        self._lock = threading.Lock()
        self._workers: dict[str, WorkerRecord] = {}

    def register(self, worker_id: str, fetch_address: str) -> bool:
        """Add a worker, or replace its record if it registers again.

        Returns True if the worker was already registered (for example, its
        container restarted before the boss noticed it was gone).
        """
        with self._lock:
            was_registered = worker_id in self._workers
            self._workers[worker_id] = WorkerRecord(worker_id, fetch_address, self._clock())
            return was_registered

    def record_heartbeat(self, worker_id: str) -> None:
        with self._lock:
            if worker_id not in self._workers:
                raise UnknownWorkerError(worker_id)
            self._workers[worker_id].last_heartbeat_time = self._clock()

    def remove_dead_workers(self) -> list[str]:
        """Remove every worker whose last heartbeat is older than the timeout, and return their IDs."""
        with self._lock:
            now = self._clock()
            dead_worker_ids = sorted(
                worker_id
                for worker_id, record in self._workers.items()
                if now - record.last_heartbeat_time > self._heartbeat_timeout_seconds
            )
            for worker_id in dead_worker_ids:
                del self._workers[worker_id]
            return dead_worker_ids

    def live_worker_ids(self) -> list[str]:
        with self._lock:
            return sorted(self._workers)
