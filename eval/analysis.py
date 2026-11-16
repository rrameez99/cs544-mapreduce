"""Turning one job's event log (boss/events.py) into the numbers the plots need.

Everything here is a pure function of a list of events, so it is unit tested
with hand-written logs (tests/test_eval.py) and never needs a cluster.
"""

import json
import statistics
from dataclasses import dataclass
from pathlib import Path


class EventLogError(Exception):
    """An event log is missing something every finished job's log must have."""


@dataclass(frozen=True)
class AttemptInterval:
    """One attempt, as a bar on a timeline: when it started, ended, and how it ended."""

    kind: str  # "map" or "reduce"
    task_id: int
    attempt_id: int
    worker_id: str
    start: float  # seconds since the job was submitted
    end: float
    outcome: str  # "accepted", "duplicate", "stale", "failed", "lost", or "unfinished"
    reason: str  # why it started: "first", "rerun", "timeout" or "backup"


def load_event_file(path: Path) -> list[dict]:
    """Load an event log saved anywhere (boss.events.read_events only reads one inside an output folder)."""
    with path.open(encoding="utf-8") as log_file:
        return [json.loads(line) for line in log_file]


def job_finished_event(events: list[dict]) -> dict:
    finished = [event for event in events if event["event"] == "job_finished"]
    if not finished:
        raise EventLogError("event log has no job_finished event; did the job finish?")
    return finished[-1]


def job_seconds(events: list[dict]) -> float:
    """Seconds from submission to the job succeeding."""
    finished = job_finished_event(events)
    if finished["state"] != "SUCCEEDED":
        raise EventLogError(f"job {finished['job_id']} {finished['state']}: {finished['failure_reason']}")
    return finished["t"]


AttemptKey = tuple[str, int, int]  # (kind, task_id, attempt_id)


def _starts_and_endings(events: list[dict]) -> tuple[dict[AttemptKey, dict], dict[AttemptKey, tuple[float, str]]]:
    """Each attempt's attempt_started event, and (time, outcome) of the first event that ended it."""
    starts: dict[AttemptKey, dict] = {}
    endings: dict[AttemptKey, tuple[float, str]] = {}
    for event in events:
        if event["event"] not in ("attempt_started", "attempt_finished", "attempt_failed", "attempt_lost"):
            continue
        key = (event["kind"], event["task_id"], event["attempt_id"])
        if event["event"] == "attempt_started":
            starts[key] = event
        elif key not in endings:  # the first ending wins; later reports are about a finished attempt
            if event["event"] == "attempt_finished":
                endings[key] = (event["t"], event["outcome"])
            elif event["event"] == "attempt_failed":
                endings[key] = (event["t"], "failed")
            else:
                endings[key] = (event["t"], "lost")
    return starts, endings


def running_attempts(events: list[dict]) -> list[dict]:
    """The attempt_started events of attempts that have not ended yet, for a log read while the job runs."""
    starts, endings = _starts_and_endings(events)
    return [start for key, start in starts.items() if key not in endings]


def lost_attempts(events: list[dict], worker_id: str, kind: str) -> list[dict]:
    """attempt_lost events for kind ("map" or "reduce") attempts that were running on worker_id when it died."""
    return [
        event for event in events
        if event["event"] == "attempt_lost" and event["worker_id"] == worker_id and event["kind"] == kind
    ]


def attempt_intervals(events: list[dict]) -> list[AttemptInterval]:
    """Every attempt of the job, from its start event to whichever event ended it.

    An attempt with no ending event (for example, the losing copy of a backup
    task that had not reported yet when the log was read) ends at job_finished
    and is marked "unfinished".
    """
    job_end = job_finished_event(events)["t"]
    starts, endings = _starts_and_endings(events)
    intervals = []
    for key, start in starts.items():
        end, outcome = endings.get(key, (job_end, "unfinished"))
        intervals.append(
            AttemptInterval(
                kind=start["kind"],
                task_id=start["task_id"],
                attempt_id=start["attempt_id"],
                worker_id=start["worker_id"],
                start=start["t"],
                end=end,
                outcome=outcome,
                reason=start["reason"],
            )
        )
    return sorted(intervals, key=lambda interval: (interval.start, interval.kind, interval.task_id))


def completion_curve(events: list[dict]) -> list[tuple[float, int]]:
    """(time, tasks completed so far) after each accepted attempt, starting from (0, 0).

    A map task whose output was lost and re-run counts again when its re-run
    is accepted, so the curve shows the work the cluster actually had to do.
    """
    accepted_times = sorted(
        event["t"] for event in events if event["event"] == "attempt_finished" and event["outcome"] == "accepted"
    )
    return [(0.0, 0)] + [(t, completed) for completed, t in enumerate(accepted_times, start=1)]


def extra_attempts(events: list[dict]) -> dict[str, int]:
    """How many attempts beyond the first each cause added: {"rerun": n, "timeout": n, "backup": n}."""
    counts = {"rerun": 0, "timeout": 0, "backup": 0}
    for event in events:
        if event["event"] == "attempt_started" and event["reason"] != "first":
            counts[event["reason"]] += 1
    return counts


@dataclass(frozen=True)
class Spread:
    median: float
    low: float
    high: float


def spread(values: list[float]) -> Spread:
    """Median with min and max, for error bars over repeated runs."""
    if not values:
        raise ValueError("no values to summarize")
    return Spread(statistics.median(values), min(values), max(values))
