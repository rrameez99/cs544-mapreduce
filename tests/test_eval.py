"""Tests for the evaluation code: event-log analysis, plot rendering, and output checking.

These never start a cluster. They use small hand-written event logs shaped like
the ones boss/jobs.py writes (see test_scheduler.py for the real thing).
"""

import json

import pytest

from eval import analysis, backup, failures, harness, scaling
from eval.analysis import AttemptInterval, EventLogError
from protos import mapreduce_pb2 as pb


def started(t, kind, task_id, attempt_id, worker_id, reason="first"):
    return {"event": "attempt_started", "job_id": "job-1", "t": t, "kind": kind, "task_id": task_id,
            "attempt_id": attempt_id, "worker_id": worker_id, "reason": reason}


def finished(t, kind, task_id, attempt_id, worker_id, outcome="accepted"):
    return {"event": "attempt_finished", "job_id": "job-1", "t": t, "kind": kind, "task_id": task_id,
            "attempt_id": attempt_id, "worker_id": worker_id, "outcome": outcome, "run_seconds": 1.0,
            "records_in": 1, "records_out": 1, "bytes_written": 10, "bytes_fetched": 0}


def job_finished(t, state="SUCCEEDED", reason=""):
    return {"event": "job_finished", "job_id": "job-1", "t": t, "state": state, "failure_reason": reason}


SUBMITTED = {"event": "job_submitted", "job_id": "job-1", "t": 0.0, "num_map_tasks": 2, "num_reduce_tasks": 1,
             "use_combiner": False, "use_backup_tasks": True, "input_files": 1, "input_bytes": 100}

# Map 0 runs on worker-1. Map 1 runs on worker-2, which dies at t=3 after finishing
# map 1, so map 1's output is lost and it is re-run on worker-3. The reduce gets a
# backup that wins; the straggling original reports later as a duplicate.
FAILURE_AND_BACKUP_LOG = [
    SUBMITTED,
    started(0.0, "map", 0, 0, "worker-1"),
    started(0.0, "map", 1, 0, "worker-2"),
    finished(1.0, "map", 0, 0, "worker-1"),
    finished(1.5, "map", 1, 0, "worker-2"),
    started(1.5, "reduce", 0, 0, "worker-4"),
    {"event": "attempt_lost", "job_id": "job-1", "t": 3.0, "kind": "reduce", "task_id": 0, "attempt_id": 0,
     "worker_id": "worker-4"},
    {"event": "map_output_lost", "job_id": "job-1", "t": 3.0, "kind": "map", "task_id": 1, "attempt_id": 0,
     "worker_id": "worker-2", "reason": "worker died"},
    started(3.0, "map", 1, 1, "worker-3", reason="rerun"),
    finished(4.0, "map", 1, 1, "worker-3"),
    started(4.0, "reduce", 0, 1, "worker-1", reason="rerun"),
    started(4.5, "reduce", 0, 2, "worker-3", reason="backup"),
    finished(5.5, "reduce", 0, 2, "worker-3"),
    job_finished(5.5),
    finished(7.0, "reduce", 0, 1, "worker-1", outcome="duplicate"),
]


# ----- analysis -----


def test_job_seconds_is_the_job_finished_time():
    assert analysis.job_seconds(FAILURE_AND_BACKUP_LOG) == 5.5


def test_job_seconds_rejects_a_failed_or_unfinished_job():
    with pytest.raises(EventLogError, match="FAILED"):
        analysis.job_seconds([SUBMITTED, job_finished(2.0, state="FAILED", reason="map task 0 failed 3 times")])
    with pytest.raises(EventLogError, match="no job_finished"):
        analysis.job_seconds([SUBMITTED])


def test_attempt_intervals_pair_each_start_with_how_it_ended():
    intervals = {(i.kind, i.task_id, i.attempt_id): i for i in analysis.attempt_intervals(FAILURE_AND_BACKUP_LOG)}
    assert len(intervals) == 6  # 2 maps + 1 map re-run + 3 reduce attempts
    assert intervals[("reduce", 0, 0)] == AttemptInterval("reduce", 0, 0, "worker-4", 1.5, 3.0, "lost", "first")
    assert intervals[("map", 1, 1)].reason == "rerun"
    assert intervals[("reduce", 0, 2)].outcome == "accepted"
    assert intervals[("reduce", 0, 1)] == AttemptInterval("reduce", 0, 1, "worker-1", 4.0, 7.0, "duplicate", "rerun")


def test_attempt_with_no_ending_event_ends_at_job_finished():
    log = [SUBMITTED, started(0.0, "map", 0, 0, "worker-1"), started(0.5, "map", 0, 1, "worker-2", reason="backup"),
           finished(2.0, "map", 0, 1, "worker-2"), job_finished(2.0)]
    straggler = next(i for i in analysis.attempt_intervals(log) if i.attempt_id == 0)
    assert (straggler.end, straggler.outcome) == (2.0, "unfinished")


def test_completion_curve_counts_each_accepted_attempt_including_reruns():
    assert analysis.completion_curve(FAILURE_AND_BACKUP_LOG) == [(0.0, 0), (1.0, 1), (1.5, 2), (4.0, 3), (5.5, 4)]


def test_extra_attempts_by_cause():
    assert analysis.extra_attempts(FAILURE_AND_BACKUP_LOG) == {"rerun": 2, "timeout": 0, "backup": 1}


def test_running_attempts_are_started_but_not_ended():
    # Partway through FAILURE_AND_BACKUP_LOG, just after the map re-run started on worker-3.
    so_far = FAILURE_AND_BACKUP_LOG[:9]
    running = analysis.running_attempts(so_far)
    assert [(e["kind"], e["task_id"], e["attempt_id"], e["worker_id"]) for e in running] == [("map", 1, 1, "worker-3")]


def test_lost_attempts_match_the_worker_and_kind():
    assert len(analysis.lost_attempts(FAILURE_AND_BACKUP_LOG, "worker-4", "reduce")) == 1
    assert analysis.lost_attempts(FAILURE_AND_BACKUP_LOG, "worker-4", "map") == []
    # worker-2 only lost finished map output (map_output_lost), not a running attempt.
    assert analysis.lost_attempts(FAILURE_AND_BACKUP_LOG, "worker-2", "map") == []


def test_spread_is_median_min_max():
    assert analysis.spread([3.0, 1.0, 2.0]) == analysis.Spread(2.0, 1.0, 3.0)
    with pytest.raises(ValueError):
        analysis.spread([])


# ----- harness helpers that don't need a cluster -----


def test_live_workers_come_from_the_latest_boss_log_line():
    log = (
        "boss INFO live workers: worker-1\n"
        "boss INFO live workers: worker-1, worker-2, worker-3\n"
        "boss WARNING worker worker-2 sent no heartbeat for over 3s; marking it dead\n"
        "boss INFO live workers: worker-1, worker-3\n"
    )
    assert harness.live_workers_from_boss_log(log) == ["worker-1", "worker-3"]
    assert harness.live_workers_from_boss_log("boss INFO boss listening on port 5440\n") == []


def test_live_events_skip_a_half_written_last_line(tmp_path):
    log = tmp_path / "_events.jsonl"
    assert harness.read_live_events(log) == []  # not created yet
    log.write_bytes(json.dumps(SUBMITTED).encode() + b"\n" + b'{"event": "attempt_sta')
    assert harness.read_live_events(log) == [SUBMITTED]


LOWERCASING_WORD_COUNT = b"""
def map(key, line):
    for word in line.split():
        yield word.lower(), 1

def reduce(key, values):
    yield key, sum(values)
"""


@pytest.fixture
def dataset(tmp_path, monkeypatch):
    """A tiny mixed-case dataset and a word count job that lowercases, like examples/wordcount.py."""
    shared = tmp_path / "shared"
    (shared / "input" / "books").mkdir(parents=True)
    (shared / "input" / "books" / "a.txt").write_bytes(b"The cat\nthe dog\n")
    (shared / "input" / "books" / "b.txt").write_bytes(b"THE Cat\r\n")
    job_file = tmp_path / "wordcount.py"
    job_file.write_bytes(LOWERCASING_WORD_COUNT)
    monkeypatch.setattr(harness, "SHARED_ROOT", shared)
    monkeypatch.setattr(harness, "DEFAULT_JOB_FILE", job_file)
    harness.reference_output.cache_clear()
    yield shared / "output"
    harness.reference_output.cache_clear()


def write_output(folder, parts):
    folder.mkdir(parents=True)
    for index, text in enumerate(parts):
        (folder / f"part-r-{index:05d}").write_bytes(text)


def test_reference_runs_the_jobs_own_map_and_reduce(dataset):
    assert harness.reference_output() == {"cat": "2", "dog": "1", "the": "3"}


def test_output_check_accepts_correct_lowercased_output(dataset):
    write_output(dataset / "good", [b"cat\t2\nthe\t3\n", b"dog\t1\n"])
    (dataset / "good" / "_events.jsonl").write_bytes(b"{}\n")  # not part of the output
    harness.check_output(dataset / "good")


def test_output_check_rejects_wrong_or_duplicated_output(dataset):
    write_output(dataset / "case-sensitive", [b"The\t1\nthe\t1\nTHE\t1\ncat\t1\nCat\t1\ndog\t1\n"])
    with pytest.raises(harness.ExperimentError, match="does not match"):
        harness.check_output(dataset / "case-sensitive")

    write_output(dataset / "duplicated", [b"cat\t2\nthe\t3\n", b"the\t3\ndog\t1\n"])
    with pytest.raises(harness.ExperimentError, match="more than one"):
        harness.check_output(dataset / "duplicated")


# ----- the failure experiment's kill trigger -----


def status(state, map_tasks_done=0):
    return pb.JobStatus(state=state, num_map_tasks=16, map_tasks_done=map_tasks_done)


def test_kill_during_map_waits_for_the_victim_to_run_a_map():
    victim_mapping = [SUBMITTED, started(0.0, "map", 0, 0, "worker-1"), started(0.0, "map", 1, 0, "worker-2")]
    victim_idle = victim_mapping + [finished(1.0, "map", 1, 0, "worker-2")]
    mapping = status(pb.JOB_STATE_MAPPING, map_tasks_done=4)

    assert failures.should_kill(failures.KILL_DURING_MAP, mapping, victim_mapping)
    assert not failures.should_kill(failures.KILL_DURING_MAP, mapping, victim_idle)
    # Too early: fewer than a quarter of the maps are done.
    assert not failures.should_kill(failures.KILL_DURING_MAP, status(pb.JOB_STATE_MAPPING, 3), victim_mapping)


def test_kill_during_reduce_waits_for_the_victim_to_run_a_reduce():
    reducing = status(pb.JOB_STATE_REDUCING, map_tasks_done=16)
    other_worker_reducing = [SUBMITTED, started(5.0, "reduce", 0, 0, "worker-1")]
    victim_reducing = other_worker_reducing + [started(5.0, "reduce", 1, 0, "worker-2")]

    assert not failures.should_kill(failures.KILL_DURING_REDUCE, reducing, other_worker_reducing)
    assert failures.should_kill(failures.KILL_DURING_REDUCE, reducing, victim_reducing)


def test_a_kill_that_lost_no_running_attempt_is_rejected(tmp_path):
    lost = {"event": "attempt_lost", "job_id": "job-1", "t": 4.0, "kind": "map", "task_id": 1, "attempt_id": 0,
            "worker_id": "worker-2"}
    no_loss = [SUBMITTED, started(0.0, "map", 1, 0, "worker-2"), finished(1.0, "map", 1, 0, "worker-2"),
               job_finished(6.0)]
    kill = {"killed_worker": "worker-2", "kill_t": 1.0}
    save_fake_results(tmp_path, "failures", [
        (failures.KILL_DURING_MAP, 1, no_loss[:-1] + [lost, no_loss[-1]], kill),
        (failures.KILL_DURING_MAP, 2, no_loss, kill),
        (failures.KILL_DURING_MAP, 3, no_loss, {}),
    ])
    lost_run, missed_run, never_killed_run = harness.load_results("failures", tmp_path)

    failures.check_kill_lost_work(failures.KILL_DURING_MAP, lost_run, tmp_path)
    with pytest.raises(harness.ExperimentError, match="lost no running map attempt"):
        failures.check_kill_lost_work(failures.KILL_DURING_MAP, missed_run, tmp_path)
    with pytest.raises(harness.ExperimentError, match="finished before worker-2 could be killed"):
        failures.check_kill_lost_work(failures.KILL_DURING_MAP, never_killed_run, tmp_path)


# ----- plots render from saved results -----


def save_fake_results(results_root, experiment, runs):
    """runs: list of (label, run, events, notes). Writes runs.json and the event logs."""
    folder = results_root / experiment
    folder.mkdir(parents=True)
    rows = []
    for label, run, events, notes in runs:
        log_name = f"{label.replace(' ', '-')}-run{run}.jsonl"
        (folder / log_name).write_text("".join(json.dumps(e) + "\n" for e in events), encoding="utf-8")
        rows.append({"experiment": experiment, "label": label, "run": run, "job_id": "job-1", "event_log": log_name,
                     "job_seconds": analysis.job_seconds(events), "notes": notes})
    (folder / "runs.json").write_text(json.dumps(rows), encoding="utf-8")


def scaled(events, factor):
    return [{**e, "t": e["t"] * factor} for e in events]


def test_scaling_plot_renders(tmp_path):
    save_fake_results(tmp_path / "results", "scaling", [
        (scaling.label_for(n), run, scaled(FAILURE_AND_BACKUP_LOG, 4 / n + run * 0.1), {})
        for n in (1, 2, 3, 4) for run in (1, 2)
    ])
    assert scaling.plot(tmp_path / "results", tmp_path / "plots").stat().st_size > 10_000


def test_failure_plots_render(tmp_path):
    kill = {"killed_worker": "worker-2", "kill_t": 2.0}
    save_fake_results(tmp_path / "results", "failures", [
        (failures.NO_FAILURE, 1, scaled(FAILURE_AND_BACKUP_LOG, 0.8), {}),
        (failures.KILL_DURING_MAP, 1, FAILURE_AND_BACKUP_LOG, kill),
        (failures.KILL_DURING_REDUCE, 1, FAILURE_AND_BACKUP_LOG, kill),
    ])
    assert failures.plot_timeline(tmp_path / "results", tmp_path / "plots").stat().st_size > 10_000
    assert failures.plot_cost(tmp_path / "results", tmp_path / "plots").stat().st_size > 10_000


def test_backup_plot_renders(tmp_path):
    save_fake_results(tmp_path / "results", "backup", [
        (backup.NORMAL, 1, scaled(FAILURE_AND_BACKUP_LOG, 1.0), {}),
        (backup.STRAGGLER, 1, scaled(FAILURE_AND_BACKUP_LOG, 2.0), {}),
        (backup.STRAGGLER_WITH_BACKUPS, 1, scaled(FAILURE_AND_BACKUP_LOG, 1.3), {}),
    ])
    assert backup.plot(tmp_path / "results", tmp_path / "plots").stat().st_size > 10_000
