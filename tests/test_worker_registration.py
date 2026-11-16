"""A worker starts from an empty local directory every time it registers.

Regression test: job IDs start again at job-1 when the boss restarts, but a
worker container keeps /local, so a new job-1 attempt collided with an old
job-1 directory (FileExistsError) and burned one of its allowed failures.
"""

from pathlib import Path

from protos import mapreduce_pb2 as pb
from worker.server import clear_local_data, register


def make_old_job_output(local_root: Path) -> None:
    attempt_directory = local_root / "job-1" / "map-0.0"
    attempt_directory.mkdir(parents=True)
    (attempt_directory / "part-0.jsonl").write_bytes(b'["the",1]\n')
    (local_root / "stray-file.tmp").write_bytes(b"x")


def test_clear_local_data_empties_the_directory_but_keeps_it(tmp_path):
    local_root = tmp_path / "local"
    make_old_job_output(local_root)

    clear_local_data(local_root)

    assert local_root.is_dir()
    assert list(local_root.iterdir()) == []


def test_clear_local_data_creates_a_missing_directory(tmp_path):
    local_root = tmp_path / "local"
    clear_local_data(local_root)
    assert local_root.is_dir()


class RecordingBoss:
    """Records what the local directory looked like when Register was called."""

    def __init__(self, local_root: Path):
        self.local_root = local_root
        self.entries_at_register = None

    def Register(self, request, timeout):
        self.entries_at_register = sorted(path.name for path in self.local_root.iterdir())
        assert request == pb.RegisterRequest(worker_id="worker-1", fetch_address="worker-1:5441")
        return pb.RegisterResponse()


def test_register_clears_old_data_before_telling_the_boss(tmp_path):
    local_root = tmp_path / "local"
    make_old_job_output(local_root)
    boss = RecordingBoss(local_root)

    register(boss, "worker-1", "worker-1:5441", local_root)

    # Empty *before* the RPC: the boss can hand out new work as soon as it
    # returns, so old directories must already be gone by then.
    assert boss.entries_at_register == []
