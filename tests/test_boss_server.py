"""Tests for the boss's RPCs, through a real gRPC server and client stubs.

The scheduling logic itself is tested in test_scheduler.py; these tests check
that the RPCs reach it correctly, validate paths, and return the right status codes.
"""

import grpc
import pytest

from boss.jobs import Scheduler
from boss.server import build_server, reschedule_work_of_dead_workers
from boss.workers import WorkerRegistry
from protos import mapreduce_pb2 as pb
from protos import mapreduce_pb2_grpc as pb_grpc

HEARTBEAT_TIMEOUT_SECONDS = 3.0
TASK_TIMEOUT_SECONDS = 30.0


class FakeClock:
    def __init__(self):
        self.now = 1000.0

    def __call__(self) -> float:
        return self.now


@pytest.fixture
def clock():
    return FakeClock()


@pytest.fixture
def shared_root(tmp_path):
    root = tmp_path / "shared"
    (root / "input").mkdir(parents=True)
    (root / "input" / "a.txt").write_bytes(b"the cat\nthe dog\n")
    return root


@pytest.fixture
def boss_state(clock):
    return WorkerRegistry(HEARTBEAT_TIMEOUT_SECONDS, clock=clock), Scheduler(TASK_TIMEOUT_SECONDS, clock=clock)


@pytest.fixture
def boss(boss_state, shared_root):
    """A running boss server; yields a client stub connected to it."""
    registry, scheduler = boss_state
    server, port = build_server(registry, scheduler, str(shared_root), "localhost:0")
    server.start()
    channel = grpc.insecure_channel(f"localhost:{port}")
    yield pb_grpc.BossStub(channel)
    channel.close()
    server.stop(grace=None)


def job_spec(shared_root, output_name="output", num_map_tasks=1, num_reduce_tasks=1, **overrides):
    fields = dict(
        input_path=str(shared_root / "input"),
        output_path=str(shared_root / output_name),
        job_file_name="wordcount.py",
        job_code=b"def map(k, v): ...\ndef reduce(k, vs): ...\n",
        num_map_tasks=num_map_tasks,
        num_reduce_tasks=num_reduce_tasks,
    )
    fields.update(overrides)
    return pb.JobSpec(**fields)


def submit(boss, spec) -> str:
    return boss.SubmitJob(pb.SubmitJobRequest(spec=spec)).job_id


def register(boss, worker_id):
    boss.Register(pb.RegisterRequest(worker_id=worker_id, fetch_address=f"{worker_id}:5441"))


def heartbeat(boss, worker_id, ready_for_task=True) -> pb.HeartbeatResponse:
    return boss.Heartbeat(pb.HeartbeatRequest(worker_id=worker_id, ready_for_task=ready_for_task))


def report_done(boss, worker_id, attempt) -> bool:
    return boss.ReportTaskDone(pb.TaskDoneReport(worker_id=worker_id, attempt=attempt)).accepted


def status_code_of(call) -> grpc.StatusCode:
    with pytest.raises(grpc.RpcError) as error:
        call()
    return error.value.code()


# ----- SubmitJob validation -----


def test_submit_creates_the_output_directory(boss, shared_root):
    submit(boss, job_spec(shared_root))
    assert (shared_root / "output").is_dir()


def test_submit_rejects_an_existing_output_directory(boss, shared_root):
    (shared_root / "output").mkdir()
    code = status_code_of(lambda: submit(boss, job_spec(shared_root)))
    assert code == grpc.StatusCode.INVALID_ARGUMENT


def test_two_jobs_cannot_share_an_output_directory(boss, shared_root):
    submit(boss, job_spec(shared_root))
    code = status_code_of(lambda: submit(boss, job_spec(shared_root)))
    assert code == grpc.StatusCode.INVALID_ARGUMENT


def test_submit_rejects_missing_input(boss, shared_root):
    spec = job_spec(shared_root, input_path=str(shared_root / "nope"))
    assert status_code_of(lambda: submit(boss, spec)) == grpc.StatusCode.INVALID_ARGUMENT


def test_submit_rejects_paths_outside_shared_storage(boss, shared_root, tmp_path):
    outside = tmp_path / "elsewhere.txt"
    outside.write_bytes(b"x\n")
    spec = job_spec(shared_root, input_path=str(outside))
    assert status_code_of(lambda: submit(boss, spec)) == grpc.StatusCode.INVALID_ARGUMENT

    spec = job_spec(shared_root, output_path=str(shared_root / ".." / "escaped"))
    assert status_code_of(lambda: submit(boss, spec)) == grpc.StatusCode.INVALID_ARGUMENT
    assert not (tmp_path / "escaped").exists()


def test_rejected_submit_does_not_leave_an_output_directory_behind(boss, shared_root):
    spec = job_spec(shared_root, num_map_tasks=0)
    assert status_code_of(lambda: submit(boss, spec)) == grpc.StatusCode.INVALID_ARGUMENT
    assert not (shared_root / "output").exists()


def test_status_of_unknown_job_is_not_found(boss):
    code = status_code_of(lambda: boss.GetJobStatus(pb.GetJobStatusRequest(job_id="job-404")))
    assert code == grpc.StatusCode.NOT_FOUND


# ----- heartbeats and assignment -----


def test_heartbeat_from_unregistered_worker_is_not_found(boss):
    assert status_code_of(lambda: heartbeat(boss, "worker-9")) == grpc.StatusCode.NOT_FOUND


def test_busy_worker_gets_no_task(boss, shared_root):
    register(boss, "worker-1")
    submit(boss, job_spec(shared_root))
    assert heartbeat(boss, "worker-1", ready_for_task=False).WhichOneof("assignment") is None


def test_whole_job_through_rpcs(boss, shared_root):
    register(boss, "worker-1")
    register(boss, "worker-2")
    job_id = submit(boss, job_spec(shared_root, num_map_tasks=2, num_reduce_tasks=1))

    maps = [heartbeat(boss, "worker-1").map_task, heartbeat(boss, "worker-2").map_task]
    assert [task.attempt.task_id for task in maps] == [0, 1]
    assert heartbeat(boss, "worker-1").WhichOneof("assignment") is None  # both maps running

    assert report_done(boss, "worker-1", maps[0].attempt)
    assert report_done(boss, "worker-2", maps[1].attempt)
    status = boss.GetJobStatus(pb.GetJobStatusRequest(job_id=job_id))
    assert status.state == pb.JOB_STATE_REDUCING

    reduce_task = heartbeat(boss, "worker-1").reduce_task
    assert [loc.fetch_address for loc in reduce_task.map_outputs] == ["worker-1:5441", "worker-2:5441"]
    assert reduce_task.output_path == str(shared_root / "output")
    assert report_done(boss, "worker-1", reduce_task.attempt)

    status = boss.GetJobStatus(pb.GetJobStatusRequest(job_id=job_id))
    assert status.state == pb.JOB_STATE_SUCCEEDED
    assert (status.map_tasks_done, status.reduce_tasks_done) == (2, 1)


def test_duplicate_completion_is_acknowledged_but_not_accepted(boss, shared_root, clock):
    register(boss, "worker-1")
    register(boss, "worker-2")
    submit(boss, job_spec(shared_root))
    slow = heartbeat(boss, "worker-1").map_task
    clock.now += TASK_TIMEOUT_SECONDS + 1
    heartbeat(boss, "worker-1", ready_for_task=False)  # worker-1 is busy but alive
    fast = heartbeat(boss, "worker-2").map_task
    assert fast.attempt.attempt_id == 1

    assert report_done(boss, "worker-2", fast.attempt) is True
    assert report_done(boss, "worker-1", slow.attempt) is False


def test_report_for_a_task_that_does_not_exist_is_invalid(boss, shared_root):
    job_id = submit(boss, job_spec(shared_root))
    bad = pb.TaskAttempt(job_id=job_id, kind=pb.TASK_KIND_MAP, task_id=99, attempt_id=0)
    assert status_code_of(lambda: report_done(boss, "worker-1", bad)) == grpc.StatusCode.INVALID_ARGUMENT


def test_failure_report_with_unfetchable_map_reruns_that_map(boss, shared_root):
    register(boss, "worker-1")
    register(boss, "worker-2")
    job_id = submit(boss, job_spec(shared_root, num_map_tasks=1, num_reduce_tasks=1))
    map_task = heartbeat(boss, "worker-1").map_task
    report_done(boss, "worker-1", map_task.attempt)
    reduce_task = heartbeat(boss, "worker-2").reduce_task

    accepted = boss.ReportTaskFailed(
        pb.TaskFailedReport(
            worker_id="worker-2",
            attempt=reduce_task.attempt,
            error="could not fetch from worker-1:5441",
            unfetchable_map_task_id=0,
        )
    ).accepted
    assert accepted
    assert boss.GetJobStatus(pb.GetJobStatusRequest(job_id=job_id)).state == pb.JOB_STATE_MAPPING
    assert heartbeat(boss, "worker-2").map_task.attempt.attempt_id == 1


# ----- worker deaths and restarts -----


def test_dead_worker_task_goes_to_another_worker(boss, boss_state, shared_root, clock):
    registry, scheduler = boss_state
    register(boss, "worker-1")
    register(boss, "worker-2")
    submit(boss, job_spec(shared_root))
    first = heartbeat(boss, "worker-1").map_task

    clock.now += HEARTBEAT_TIMEOUT_SECONDS / 2
    heartbeat(boss, "worker-2", ready_for_task=False)
    clock.now += HEARTBEAT_TIMEOUT_SECONDS / 2 + 1  # worker-1 silent for too long, worker-2 not
    reschedule_work_of_dead_workers(registry, scheduler)

    assert registry.live_worker_ids() == ["worker-2"]
    second = heartbeat(boss, "worker-2").map_task  # no need to wait for the task timeout
    assert (second.attempt.task_id, second.attempt.attempt_id) == (first.attempt.task_id, 1)


def test_restarted_worker_loses_its_tasks(boss, shared_root):
    register(boss, "worker-1")
    register(boss, "worker-2")
    submit(boss, job_spec(shared_root))
    first = heartbeat(boss, "worker-1").map_task

    register(boss, "worker-1")  # container restarted with the same ID

    second = heartbeat(boss, "worker-2").map_task
    assert second.attempt.attempt_id == 1
    assert report_done(boss, "worker-1", first.attempt) is False
