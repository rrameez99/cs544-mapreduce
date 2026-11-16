"""Per-job event log: one JSON line per thing that happens to a job.

The scheduler records every attempt start, finish, failure and loss here, with
the time since the job was submitted and the worker's metrics. The evaluation
plots (scaling, failure timeline, combiner bytes, backup tasks) are all built
from these files, so the boss never needs extra instrumentation for them.

Each job's log is written next to its output, as <output_path>/_events.jsonl.
The output directory is new for every job, so a log can never mix two runs,
even when job IDs start again at job-1 after the boss restarts. The leading
underscore keeps it apart from the part-r-* result files.
"""

import json
import os
from typing import Protocol

EVENT_LOG_FILE_NAME = "_events.jsonl"


class EventLog(Protocol):
    def record(self, output_path: str, event: dict) -> None:
        """Store one event for the job whose output directory is output_path."""


class JsonlEventLog:
    """Appends each event as one JSON line to <output_path>/_events.jsonl."""

    def record(self, output_path: str, event: dict) -> None:
        with open(os.path.join(output_path, EVENT_LOG_FILE_NAME), "a", encoding="utf-8") as log_file:
            log_file.write(json.dumps(event) + "\n")


def read_events(output_path: str) -> list[dict]:
    """Load every event of one job, in the order they were recorded."""
    with open(os.path.join(output_path, EVENT_LOG_FILE_NAME), encoding="utf-8") as log_file:
        return [json.loads(line) for line in log_file]
