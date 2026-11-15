"""Unit tests for boss/jobs.py: splits, assignment order, timeouts, stale and
duplicate reports, worker deaths, and task failures. Uses a fake clock."""

import pytest

from boss.jobs import (
    MAX_FAILURES_PER_TASK,
    InvalidJobError,
    InvalidReportError,
    Scheduler,
    UnknownJobError,
    compute_splits,
    list_input_files,
)
from protos import mapreduce_pb2 as pb

TIMEOUT_SECONDS = 30.0


class FakeClock:
    def __init__(self):
        self.now = 1000.0

    def __call__(self) -> float:
        return self.now


@pytest.fixture
def clock():
    return FakeClock()


class RecordingEventLog:
    """Keeps events in memory instead of writing files."""

    def __init__(self):
        self.events = []

    def record(self, output_path: str, event: dict) -> None:
        self.events.append(event)

    def named(self, event_name: str) -> list[dict]:
        return [event for event in self.events if event["event"] == event_name]


@pytest.fixture
def events():
    return RecordingEventLog()


@pytest.fixture
def scheduler(clock, events):
    return Scheduler(TIMEOUT_SECONDS, clock=clock, events=events)


def make_spec(num_map_tasks=2, num_reduce_tasks=2) -> pb.JobSpec:
    return pb.JobSpec(
        input_path="/shared/input",
        output_path="/shared/output",
        job_file_name="wordcount.py",
        job_code=b"def map(k, v): ...\ndef reduce(k, vs): ...\n",
        num_map_tasks=num_map_tasks,
        num_reduce_tasks=num_reduce_tasks,
    )


def submit(scheduler, num_map_tasks=2, num_reduce_tasks=2, input_bytes=100) -> str:
    return scheduler.submit_job(
        make_spec(num_map_tasks, num_reduce_tasks), [("/shared/input/a.txt", input_bytes)]
    )


def assign(scheduler, worker_id):
    return scheduler.assign_task(worker_id, f"{worker_id}:5441")


def finish(scheduler, worker_id, task, metrics: pb.TaskMetrics | None = None) -> bool:
    if metrics is None:
        metrics = pb.TaskMetrics()
    return scheduler.report_task_done(worker_id, task.attempt, metrics)


def run_all_maps(scheduler, num_map_tasks):
    """Assign every map task to its own worker and finish it. Returns the tasks."""
    tasks = [assign(scheduler, f"worker-{i + 1}") for i in range(num_map_tasks)]
    for i, task in enumerate(tasks):
        assert finish(scheduler, f"worker-{i + 1}", task)
    return tasks


# ----- input splits -----


def covered_bytes(splits, input_files):
    """Map each file to the sorted list of byte offsets the splits cover."""
    covered = {path: [] for path, _ in input_files}
    for task_splits in splits:
        for split in task_splits:
            covered[split.path].extend(range(split.start_offset, split.end_offset))
    return covered


@pytest.mark.parametrize("num_map_tasks", [1, 2, 3, 7, 50])
def test_splits_cover_every_byte_exactly_once(num_map_tasks):
    input_files = [("/in/a.txt", 10), ("/in/b.txt", 0), ("/in/c.txt", 23)]
    splits = compute_splits(input_files, num_map_tasks)

    assert len(splits) == num_map_tasks
    covered = covered_bytes(splits, input_files)
    for path, size in input_files:
        assert sorted(covered[path]) == list(range(size))


def test_splits_are_balanced_and_span_files():
    splits = compute_splits([("/in/a.txt", 6), ("/in/b.txt", 6)], 3)
    sizes = [sum(s.end_offset - s.start_offset for s in task_splits) for task_splits in splits]
    assert sizes == [4, 4, 4]
    # The middle task's range crosses from a.txt into b.txt.
    assert [(s.path, s.start_offset, s.end_offset) for s in splits[1]] == [
        ("/in/a.txt", 4, 6),
        ("/in/b.txt", 0, 2),
    ]


def test_more_map_tasks_than_bytes_leaves_some_tasks_empty():
    splits = compute_splits([("/in/a.txt", 2)], 4)
    assert sum(1 for task_splits in splits if task_splits) == 2


def test_list_input_files_reads_a_directory_in_sorted_order(tmp_path):
    # write_bytes, not write_text: on Windows write_text turns "\n" into "\r\n",
    # which changes the file sizes.
    (tmp_path / "b.txt").write_bytes(b"hello\n")
    (tmp_path / "a.txt").write_bytes(b"hi\n")
    assert list_input_files(str(tmp_path)) == [
        (str(tmp_path / "a.txt"), 3),
        (str(tmp_path / "b.txt"), 6),
    ]


def test_list_input_files_rejects_missing_or_empty_input(tmp_path):
    with pytest.raises(InvalidJobError):
        list_input_files(str(tmp_path / "missing"))
    with pytest.raises(InvalidJobError):
        list_input_files(str(tmp_path))  # exists but has no files


# ----- submission and status -----


@pytest.mark.parametrize("num_map_tasks, num_reduce_tasks", [(0, 1), (1, 0)])
def test_submit_rejects_bad_task_counts(scheduler, num_map_tasks, num_reduce_tasks):
    with pytest.raises(InvalidJobError):
        scheduler.submit_job(make_spec(num_map_tasks, num_reduce_tasks), [("/in/a.txt", 10)])


def test_status_of_unknown_job_raises(scheduler):
    with pytest.raises(UnknownJobError):
        scheduler.job_status("job-404")


def test_job_runs_through_map_and_reduce_phases(scheduler):
    job_id = submit(scheduler, num_map_tasks=2, num_reduce_tasks=2)
    assert scheduler.job_status(job_id).state == pb.JOB_STATE_MAPPING

    run_all_maps(scheduler, 2)
    status = scheduler.job_status(job_id)
    assert status.state == pb.JOB_STATE_REDUCING
    assert status.map_tasks_done == 2

    reduces = [assign(scheduler, "worker-1"), assign(scheduler, "worker-2")]
    assert all(isinstance(task, pb.ReduceTask) for task in reduces)
    for worker_id, task in zip(["worker-1", "worker-2"], reduces):
        assert finish(scheduler, worker_id, task)

    status = scheduler.job_status(job_id)
    assert status.state == pb.JOB_STATE_SUCCEEDED
    assert status.reduce_tasks_done == 2
    assert assign(scheduler, "worker-3") is None


# ----- assignment -----


def test_no_reduce_task_until_every_map_is_done(scheduler):
    submit(scheduler, num_map_tasks=2, num_reduce_tasks=1)
    first_map = assign(scheduler, "worker-1")
    assign(scheduler, "worker-2")
    finish(scheduler, "worker-1", first_map)

    # One map is still running and hasn't timed out, so there's nothing to hand out.
    assert assign(scheduler, "worker-3") is None


def test_reduce_task_gets_the_accepted_location_of_every_map(scheduler):
    submit(scheduler, num_map_tasks=2, num_reduce_tasks=1)
    run_all_maps(scheduler, 2)

    reduce_task = assign(scheduler, "worker-3")
    assert [(loc.map_task_id, loc.attempt_id, loc.fetch_address) for loc in reduce_task.map_outputs] == [
        (0, 0, "worker-1:5441"),
        (1, 0, "worker-2:5441"),
    ]


def test_map_task_carries_its_splits_and_job_settings(scheduler):
    submit(scheduler, num_map_tasks=1, num_reduce_tasks=3, input_bytes=40)
    task = assign(scheduler, "worker-1")
    assert task.attempt.kind == pb.TASK_KIND_MAP
    assert task.num_reduce_tasks == 3
    assert [(s.start_offset, s.end_offset) for s in task.splits] == [(0, 40)]
    assert task.job_code == make_spec().job_code


def test_jobs_are_served_in_submission_order(scheduler):
    first_job = submit(scheduler, num_map_tasks=1, num_reduce_tasks=1)
    second_job = submit(scheduler, num_map_tasks=1, num_reduce_tasks=1)
    assert assign(scheduler, "worker-1").attempt.job_id == first_job
    assert assign(scheduler, "worker-2").attempt.job_id == second_job


# ----- timeouts -----


def test_timed_out_task_gets_a_new_attempt_on_another_worker(scheduler, clock):
    submit(scheduler, num_map_tasks=1, num_reduce_tasks=1)
    first = assign(scheduler, "worker-1")

    clock.now += TIMEOUT_SECONDS  # exactly at the timeout: not yet
    assert assign(scheduler, "worker-2") is None

    clock.now += 1
    assert assign(scheduler, "worker-1") is None  # never retry on the same worker
    second = assign(scheduler, "worker-2")
    assert second.attempt.task_id == first.attempt.task_id
    assert second.attempt.attempt_id == 1


def test_first_attempt_to_finish_wins_and_the_other_is_a_duplicate(scheduler, clock):
    job_id = submit(scheduler, num_map_tasks=1, num_reduce_tasks=1)
    slow = assign(scheduler, "worker-1")
    clock.now += TIMEOUT_SECONDS + 1
    fast = assign(scheduler, "worker-2")

    assert finish(scheduler, "worker-2", fast) is True
    assert finish(scheduler, "worker-1", slow) is False
    assert scheduler.duplicate_reports(job_id) == 1

    reduce_task = assign(scheduler, "worker-3")
    assert reduce_task.map_outputs[0].fetch_address == "worker-2:5441"


def test_a_timed_out_attempt_can_still_win(scheduler, clock):
    submit(scheduler, num_map_tasks=1, num_reduce_tasks=1)
    slow = assign(scheduler, "worker-1")
    clock.now += TIMEOUT_SECONDS + 1
    assign(scheduler, "worker-2")

    assert finish(scheduler, "worker-1", slow) is True
    assert assign(scheduler, "worker-3").map_outputs[0].fetch_address == "worker-1:5441"


# ----- stale reports -----


def test_report_from_a_worker_that_was_not_given_the_attempt_is_ignored(scheduler):
    submit(scheduler, num_map_tasks=1, num_reduce_tasks=1)
    task = assign(scheduler, "worker-1")
    assert finish(scheduler, "worker-2", task) is False
    assert finish(scheduler, "worker-1", task) is True


def test_report_for_an_attempt_that_was_never_assigned_is_ignored(scheduler):
    job_id = submit(scheduler, num_map_tasks=1, num_reduce_tasks=1)
    made_up = pb.TaskAttempt(job_id=job_id, kind=pb.TASK_KIND_MAP, task_id=0, attempt_id=7)
    assert scheduler.report_task_done("worker-1", made_up, pb.TaskMetrics()) is False


def test_report_for_a_task_that_does_not_exist_raises(scheduler):
    job_id = submit(scheduler, num_map_tasks=1, num_reduce_tasks=1)
    bad = pb.TaskAttempt(job_id=job_id, kind=pb.TASK_KIND_MAP, task_id=5, attempt_id=0)
    with pytest.raises(InvalidReportError):
        scheduler.report_task_done("worker-1", bad, pb.TaskMetrics())


def test_report_from_a_dead_worker_is_ignored(scheduler):
    submit(scheduler, num_map_tasks=1, num_reduce_tasks=1)
    task = assign(scheduler, "worker-1")
    scheduler.handle_dead_worker("worker-1")
    assert finish(scheduler, "worker-1", task) is False


# ----- dead workers -----


def test_dead_worker_running_task_becomes_idle_again(scheduler):
    submit(scheduler, num_map_tasks=1, num_reduce_tasks=1)
    first = assign(scheduler, "worker-1")
    scheduler.handle_dead_worker("worker-1")

    second = assign(scheduler, "worker-2")  # no need to wait for the timeout
    assert second.attempt.task_id == first.attempt.task_id
    assert second.attempt.attempt_id == 1


def test_completed_maps_on_a_dead_worker_run_again_during_reduce(scheduler):
    job_id = submit(scheduler, num_map_tasks=2, num_reduce_tasks=1)
    run_all_maps(scheduler, 2)  # map 0 on worker-1, map 1 on worker-2
    assert scheduler.job_status(job_id).state == pb.JOB_STATE_REDUCING

    scheduler.handle_dead_worker("worker-1")
    assert scheduler.job_status(job_id).state == pb.JOB_STATE_MAPPING

    redo = assign(scheduler, "worker-3")
    assert isinstance(redo, pb.MapTask)
    assert redo.attempt.task_id == 0
    finish(scheduler, "worker-3", redo)

    reduce_task = assign(scheduler, "worker-2")
    assert reduce_task.map_outputs[0].fetch_address == "worker-3:5441"


def test_dead_worker_does_not_affect_a_finished_job(scheduler):
    job_id = submit(scheduler, num_map_tasks=1, num_reduce_tasks=1)
    run_all_maps(scheduler, 1)
    finish(scheduler, "worker-2", assign(scheduler, "worker-2"))
    assert scheduler.job_status(job_id).state == pb.JOB_STATE_SUCCEEDED

    scheduler.handle_dead_worker("worker-1")
    assert scheduler.job_status(job_id).state == pb.JOB_STATE_SUCCEEDED


# ----- failures -----


def fail(scheduler, worker_id, task, error="ZeroDivisionError in map()", unfetchable_map_task_id=None) -> bool:
    return scheduler.report_task_failed(worker_id, task.attempt, error, unfetchable_map_task_id)


def test_failed_task_is_retried(scheduler):
    submit(scheduler, num_map_tasks=1, num_reduce_tasks=1)
    first = assign(scheduler, "worker-1")
    assert fail(scheduler, "worker-1", first)

    second = assign(scheduler, "worker-1")
    assert second.attempt.attempt_id == 1


def test_job_fails_after_a_task_fails_too_many_times(scheduler):
    job_id = submit(scheduler, num_map_tasks=1, num_reduce_tasks=1)
    for _ in range(MAX_FAILURES_PER_TASK):
        fail(scheduler, "worker-1", assign(scheduler, "worker-1"), error="KeyError: 'x'")

    status = scheduler.job_status(job_id)
    assert status.state == pb.JOB_STATE_FAILED
    assert "map task 0 failed 3 times" in status.failure_reason
    assert "KeyError: 'x'" in status.failure_reason
    assert assign(scheduler, "worker-1") is None


def test_failure_of_a_backup_after_the_task_succeeded_is_harmless(scheduler, clock):
    job_id = submit(scheduler, num_map_tasks=1, num_reduce_tasks=1)
    slow = assign(scheduler, "worker-1")
    clock.now += TIMEOUT_SECONDS + 1
    fast = assign(scheduler, "worker-2")
    finish(scheduler, "worker-2", fast)

    for _ in range(MAX_FAILURES_PER_TASK):
        fail(scheduler, "worker-1", slow)
    assert scheduler.job_status(job_id).state == pb.JOB_STATE_REDUCING


def test_unfetchable_map_output_reruns_the_map_without_counting_against_the_reduce(scheduler):
    job_id = submit(scheduler, num_map_tasks=2, num_reduce_tasks=1)
    run_all_maps(scheduler, 2)
    reduce_task = assign(scheduler, "worker-3")

    assert fail(scheduler, "worker-3", reduce_task, error="connection refused", unfetchable_map_task_id=1)
    assert scheduler.job_status(job_id).state == pb.JOB_STATE_MAPPING

    redo = assign(scheduler, "worker-3")
    assert redo.attempt.kind == pb.TASK_KIND_MAP and redo.attempt.task_id == 1
    finish(scheduler, "worker-3", redo)

    retry = assign(scheduler, "worker-4")
    assert retry.attempt.kind == pb.TASK_KIND_REDUCE
    assert retry.map_outputs[1].fetch_address == "worker-3:5441"


def test_unfetchable_report_with_an_old_location_does_not_rerun_the_map_again(scheduler):
    submit(scheduler, num_map_tasks=1, num_reduce_tasks=2)
    run_all_maps(scheduler, 1)  # map 0 on worker-1
    reduce_a = assign(scheduler, "worker-2")
    reduce_b = assign(scheduler, "worker-3")

    # Reducer A reports the map unreachable; the map is re-run on worker-4.
    fail(scheduler, "worker-2", reduce_a, unfetchable_map_task_id=0)
    finish(scheduler, "worker-4", assign(scheduler, "worker-4"))

    # Reducer B started before the re-run, so it had worker-1's address too.
    # Its report must not throw away the fresh output on worker-4.
    fail(scheduler, "worker-3", reduce_b, unfetchable_map_task_id=0)
    next_task = assign(scheduler, "worker-2")
    assert next_task.attempt.kind == pb.TASK_KIND_REDUCE
    assert next_task.map_outputs[0].fetch_address == "worker-4:5441"


# ----- event log -----


def test_events_trace_a_whole_job(scheduler, events, clock):
    job_id = submit(scheduler, num_map_tasks=1, num_reduce_tasks=1, input_bytes=100)
    clock.now += 1
    map_task = assign(scheduler, "worker-1")
    clock.now += 2
    finish(scheduler, "worker-1", map_task, pb.TaskMetrics(records_in=5, records_out=9, bytes_written=120))
    finish(scheduler, "worker-2", assign(scheduler, "worker-2"))

    assert [event["event"] for event in events.events] == [
        "job_submitted",
        "attempt_started",
        "attempt_finished",
        "attempt_started",
        "attempt_finished",
        "job_finished",
    ]
    assert all(event["job_id"] == job_id for event in events.events)
    assert events.named("job_submitted")[0]["input_bytes"] == 100

    map_start, reduce_start = events.named("attempt_started")
    assert (map_start["kind"], map_start["worker_id"], map_start["reason"], map_start["t"]) == (
        "map", "worker-1", "first", 1.0
    )
    map_finish = events.named("attempt_finished")[0]
    assert (map_finish["outcome"], map_finish["t"]) == ("accepted", 3.0)
    assert (map_finish["records_in"], map_finish["records_out"], map_finish["bytes_written"]) == (5, 9, 120)
    assert reduce_start["kind"] == "reduce"
    assert events.named("job_finished")[0]["state"] == "SUCCEEDED"


def test_events_mark_timeout_retries_and_duplicates(scheduler, events, clock):
    submit(scheduler, num_map_tasks=1, num_reduce_tasks=1)
    slow = assign(scheduler, "worker-1")
    clock.now += TIMEOUT_SECONDS + 1
    fast = assign(scheduler, "worker-2")
    finish(scheduler, "worker-2", fast)
    finish(scheduler, "worker-1", slow)

    assert [event["reason"] for event in events.named("attempt_started")] == ["first", "timeout"]
    assert [(event["worker_id"], event["outcome"]) for event in events.named("attempt_finished")] == [
        ("worker-2", "accepted"),
        ("worker-1", "duplicate"),
    ]


def test_events_record_work_lost_with_a_dead_worker(scheduler, events):
    submit(scheduler, num_map_tasks=2, num_reduce_tasks=1)
    run_all_maps(scheduler, 2)  # map 0 on worker-1, map 1 on worker-2
    reduce_task = assign(scheduler, "worker-1")

    scheduler.handle_dead_worker("worker-1")
    redo = assign(scheduler, "worker-3")

    lost = events.named("attempt_lost")
    assert [(e["kind"], e["task_id"], e["worker_id"]) for e in lost] == [("reduce", 0, "worker-1")]
    assert lost[0]["attempt_id"] == reduce_task.attempt.attempt_id
    output_lost = events.named("map_output_lost")
    assert [(e["task_id"], e["worker_id"], e["reason"]) for e in output_lost] == [(0, "worker-1", "worker died")]
    assert events.named("attempt_started")[-1]["reason"] == "rerun"
    assert redo.attempt.task_id == 0


def test_events_record_failures_and_the_failed_job(scheduler, events):
    submit(scheduler, num_map_tasks=1, num_reduce_tasks=1)
    for _ in range(MAX_FAILURES_PER_TASK):
        fail(scheduler, "worker-1", assign(scheduler, "worker-1"), error="KeyError: 'x'")

    failures = events.named("attempt_failed")
    assert [event["outcome"] for event in failures] == ["counted"] * MAX_FAILURES_PER_TASK
    assert failures[0]["error"] == "KeyError: 'x'"
    finished = events.named("job_finished")
    assert finished[0]["state"] == "FAILED"
    assert "KeyError" in finished[0]["failure_reason"]


def test_events_record_unfetchable_map_output(scheduler, events):
    submit(scheduler, num_map_tasks=1, num_reduce_tasks=1)
    run_all_maps(scheduler, 1)
    reduce_task = assign(scheduler, "worker-2")
    fail(scheduler, "worker-2", reduce_task, error="connection refused", unfetchable_map_task_id=0)

    assert events.named("attempt_failed")[0]["outcome"] == "map output unfetchable"
    assert events.named("attempt_failed")[0]["unfetchable_map_task_id"] == 0
    assert [(e["task_id"], e["reason"]) for e in events.named("map_output_lost")] == [(0, "unfetchable")]


def test_stale_reports_are_logged_as_stale(scheduler, events):
    submit(scheduler, num_map_tasks=1, num_reduce_tasks=1)
    task = assign(scheduler, "worker-1")
    finish(scheduler, "worker-9", task)  # not the worker that was given this attempt
    assert events.named("attempt_finished")[0]["outcome"] == "stale"
