"""Unit tests for the boss's worker registry, using a fake clock instead of real time."""

import pytest

from boss.workers import UnknownWorkerError, WorkerRegistry

TIMEOUT_SECONDS = 3.0


class FakeClock:
    def __init__(self):
        self.now = 100.0

    def __call__(self) -> float:
        return self.now


@pytest.fixture
def clock():
    return FakeClock()


@pytest.fixture
def registry(clock):
    return WorkerRegistry(TIMEOUT_SECONDS, clock=clock)


def test_registered_workers_are_live(registry):
    assert registry.register("worker-1", "worker-1:5441") is False
    assert registry.register("worker-2", "worker-2:5441") is False
    assert registry.live_worker_ids() == ["worker-1", "worker-2"]


def test_registering_again_reports_it_was_already_registered(registry):
    registry.register("worker-1", "worker-1:5441")
    assert registry.register("worker-1", "worker-1:5441") is True
    assert registry.live_worker_ids() == ["worker-1"]


def test_heartbeat_returns_the_workers_fetch_address(registry):
    registry.register("worker-1", "worker-1:5441")
    assert registry.record_heartbeat("worker-1") == "worker-1:5441"


def test_heartbeat_from_unregistered_worker_raises(registry):
    with pytest.raises(UnknownWorkerError):
        registry.record_heartbeat("worker-9")


def test_worker_without_heartbeats_is_removed_after_timeout(registry, clock):
    registry.register("worker-1", "worker-1:5441")

    clock.now += TIMEOUT_SECONDS  # exactly at the timeout: still alive
    assert registry.remove_dead_workers() == []

    clock.now += 0.1
    assert registry.remove_dead_workers() == ["worker-1"]
    assert registry.live_worker_ids() == []


def test_heartbeats_keep_a_worker_alive(registry, clock):
    registry.register("worker-1", "worker-1:5441")
    registry.register("worker-2", "worker-2:5441")

    for _ in range(5):
        clock.now += 2.0
        registry.record_heartbeat("worker-1")  # worker-2 stays silent

    assert registry.remove_dead_workers() == ["worker-2"]
    assert registry.live_worker_ids() == ["worker-1"]


def test_dead_worker_must_register_again(registry, clock):
    registry.register("worker-1", "worker-1:5441")
    clock.now += TIMEOUT_SECONDS + 1
    registry.remove_dead_workers()

    with pytest.raises(UnknownWorkerError):
        registry.record_heartbeat("worker-1")

    assert registry.register("worker-1", "worker-1:5441") is False
    registry.record_heartbeat("worker-1")
