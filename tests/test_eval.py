"""Tests for the evaluation code: event-log analysis, plot rendering, and output checking.

These never start a cluster. They use small hand-written event logs shaped like
the ones boss/jobs.py writes (see test_scheduler.py for the real thing).
"""

import json

import pytest

from eval import analysis, backup, failures, harness, scaling
from eval.analysis import AttemptInterval, EventLogError


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


def test_word_count_output_check(tmp_path, monkeypatch):
    shared = tmp_path / "shared"
    (shared / "input" / "books").mkdir(parents=True)
    (shared / "input" / "books" / "a.txt").write_bytes(b"the cat\nthe dog\n")
    monkeypatch.setattr(harness, "SHARED_ROOT", shared)
    harness.reference_word_count.cache_clear()

    good = shared / "output" / "good"
    good.mkdir(parents=True)
    (good / "part-r-00000").write_bytes(b"cat\t1\nthe\t2\n")
    (good / "part-r-00001").write_bytes(b"dog\t1\n")
    (good / "_events.jsonl").write_bytes(b"{}\n")  # not part of the output
    harness.check_word_count_output(good)

    wrong = shared / "output" / "wrong"
    wrong.mkdir()
    (wrong / "part-r-00000").write_bytes(b"cat\t1\nthe\t1\ndog\t1\n")
    with pytest.raises(harness.ExperimentError, match="does not match"):
        harness.check_word_count_output(wrong)

    duplicated = shared / "output" / "duplicated"
    duplicated.mkdir()
    (duplicated / "part-r-00000").write_bytes(b"cat\t1\nthe\t2\n")
    (duplicated / "part-r-00001").write_bytes(b"the\t2\ndog\t1\n")
    with pytest.raises(harness.ExperimentError, match="more than one"):
        harness.check_word_count_output(duplicated)
    harness.reference_word_count.cache_clear()


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
