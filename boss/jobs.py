"""Job and task bookkeeping for the boss.

Tracks every job's map and reduce tasks, which attempts are running where,
and decides what to hand out next. It is plain Python with no gRPC, so it can
be unit tested with a fake clock; boss/server.py owns one Scheduler and calls
it from its RPC handlers.

Task states are never stored, only derived (see Task.state), so they cannot
disagree with the attempts:
  - DONE        a completed attempt was accepted
  - IN_PROGRESS no accepted attempt, at least one attempt running
  - IDLE        neither
"""

import itertools
import logging
import math
import os
import threading
from dataclasses import dataclass, field
from enum import Enum
from typing import Callable

from boss.events import EventLog
from protos import mapreduce_pb2 as pb

# A task whose attempts keep failing (e.g. the user's map() raises) fails the job.
MAX_FAILURES_PER_TASK = 3
# Backup tasks (paper §3.6) start once at most this fraction of a phase's tasks
# (and at least one) is unfinished, and every unfinished task is already running.
BACKUP_PHASE_FRACTION = 0.1

log = logging.getLogger("boss.jobs")


class InvalidJobError(Exception):
    """The submitted job cannot run: bad task counts, missing input, etc."""


class UnknownJobError(Exception):
    """No job with this ID was ever submitted."""


class InvalidReportError(Exception):
    """A worker sent a report that no correct worker could send (e.g. an out-of-range task ID)."""


class TaskState(Enum):
    IDLE = "idle"
    IN_PROGRESS = "in progress"
    DONE = "done"


@dataclass
class Attempt:
    attempt_id: int
    worker_id: str
    fetch_address: str
    start_time: float
    # Reduce attempts only: for each map task, the attempt whose output this
    # reducer was told to fetch.
    map_attempt_ids: tuple[int, ...] = ()


@dataclass
class Task:
    kind: int  # pb.TASK_KIND_MAP or pb.TASK_KIND_REDUCE
    task_id: int
    running_attempts: dict[int, Attempt] = field(default_factory=dict)
    accepted_attempt: Attempt | None = None
    next_attempt_id: int = 0
    failure_count: int = 0

    @property
    def state(self) -> TaskState:
        if self.accepted_attempt is not None:
            return TaskState.DONE
        if self.running_attempts:
            return TaskState.IN_PROGRESS
        return TaskState.IDLE

    @property
    def kind_name(self) -> str:
        return "map" if self.kind == pb.TASK_KIND_MAP else "reduce"

    @property
    def name(self) -> str:
        return f"{self.kind_name} task {self.task_id}"


@dataclass
class Job:
    job_id: str
    spec: pb.JobSpec
    map_splits: list[list[pb.InputSplit]]
    map_tasks: list[Task]
    reduce_tasks: list[Task]
    submit_time: float
    failure_reason: str = ""  # non-empty once the job has failed
    duplicate_reports: int = 0  # completions ignored because the task was already done

    @property
    def state(self) -> int:
        """The job's pb.JobState, derived from its tasks."""
        if self.failure_reason:
            return pb.JOB_STATE_FAILED
        if all(task.state == TaskState.DONE for task in self.reduce_tasks):
            return pb.JOB_STATE_SUCCEEDED
        if all(task.state == TaskState.DONE for task in self.map_tasks):
            return pb.JOB_STATE_REDUCING
        return pb.JOB_STATE_MAPPING

    @property
    def is_finished(self) -> bool:
        return self.state in (pb.JOB_STATE_SUCCEEDED, pb.JOB_STATE_FAILED)


def list_input_files(input_path: str) -> list[tuple[str, int]]:
    """Return (path, size in bytes) for the input: one file, or every file in a directory, sorted by path."""
    if os.path.isfile(input_path):
        return [(input_path, os.path.getsize(input_path))]
    if os.path.isdir(input_path):
        paths = sorted(
            os.path.join(input_path, name)
            for name in os.listdir(input_path)
            if os.path.isfile(os.path.join(input_path, name))
        )
        if not paths:
            raise InvalidJobError(f"input directory {input_path} contains no files")
        return [(path, os.path.getsize(path)) for path in paths]
    raise InvalidJobError(f"input path {input_path} does not exist")


def compute_splits(input_files: list[tuple[str, int]], num_map_tasks: int) -> list[list[pb.InputSplit]]:
    """Divide the input bytes as evenly as possible into num_map_tasks byte ranges.

    The files are treated as one long byte sequence in the given order. Map
    task i covers bytes [i * total // M, (i + 1) * total // M) of it; a range
    that spans several files becomes several InputSplits. If there are more
    tasks than bytes, some tasks get no splits. They still run, and write
    empty partition files, so every reducer finds output from every map task.
    """
    total_bytes = sum(size for _, size in input_files)
    splits: list[list[pb.InputSplit]] = [[] for _ in range(num_map_tasks)]
    file_start = 0
    for path, size in input_files:
        file_end = file_start + size
        for task_id in range(num_map_tasks):
            range_start = task_id * total_bytes // num_map_tasks
            range_end = (task_id + 1) * total_bytes // num_map_tasks
            overlap_start = max(range_start, file_start)
            overlap_end = min(range_end, file_end)
            if overlap_start < overlap_end:
                splits[task_id].append(
                    pb.InputSplit(
                        path=path,
                        start_offset=overlap_start - file_start,
                        end_offset=overlap_end - file_start,
                    )
                )
        file_start = file_end
    return splits


class Scheduler:
    """All job and task state. Every public method is thread-safe.

    `clock` returns the current time in seconds; tests pass a fake clock.
    Everything that happens to a job is also recorded in `events` (see
    boss/events.py), which the evaluation plots are built from.
    """

    def __init__(self, task_timeout_seconds: float, clock: Callable[[], float], events: EventLog):
        self._task_timeout_seconds = task_timeout_seconds
        self._clock = clock
        self._events = events
        self._dropped_events = 0  # events the log could not store (see _record)
        self._lock = threading.Lock()
        self._jobs: dict[str, Job] = {}  # insertion order = submission order, so jobs run FIFO
        self._job_numbers = itertools.count(1)

    # ----- client-facing -----

    def submit_job(self, spec: pb.JobSpec, input_files: list[tuple[str, int]]) -> str:
        if spec.num_map_tasks < 1:
            raise InvalidJobError(f"num_map_tasks must be at least 1, got {spec.num_map_tasks}")
        if spec.num_reduce_tasks < 1:
            raise InvalidJobError(f"num_reduce_tasks must be at least 1, got {spec.num_reduce_tasks}")
        if not spec.job_code:
            raise InvalidJobError("job_code is empty")

        total_bytes = sum(size for _, size in input_files)
        with self._lock:
            job_id = f"job-{next(self._job_numbers)}"
            job = Job(
                job_id=job_id,
                spec=spec,
                map_splits=compute_splits(input_files, spec.num_map_tasks),
                map_tasks=[Task(pb.TASK_KIND_MAP, i) for i in range(spec.num_map_tasks)],
                reduce_tasks=[Task(pb.TASK_KIND_REDUCE, r) for r in range(spec.num_reduce_tasks)],
                submit_time=self._clock(),
            )
            self._jobs[job_id] = job
            self._record(
                job,
                "job_submitted",
                num_map_tasks=spec.num_map_tasks,
                num_reduce_tasks=spec.num_reduce_tasks,
                use_combiner=spec.use_combiner,
                use_backup_tasks=spec.use_backup_tasks,
                input_files=len(input_files),
                input_bytes=total_bytes,
            )
        log.info(
            "%s submitted: %d input files (%d bytes), %d map tasks, %d reduce tasks",
            job_id, len(input_files), total_bytes, spec.num_map_tasks, spec.num_reduce_tasks,
        )
        return job_id

    def job_status(self, job_id: str) -> pb.JobStatus:
        with self._lock:
            job = self._get_job(job_id)
            return pb.JobStatus(
                job_id=job_id,
                state=job.state,
                map_tasks_done=sum(task.state == TaskState.DONE for task in job.map_tasks),
                num_map_tasks=len(job.map_tasks),
                reduce_tasks_done=sum(task.state == TaskState.DONE for task in job.reduce_tasks),
                num_reduce_tasks=len(job.reduce_tasks),
                failure_reason=job.failure_reason,
            )

    # ----- worker-facing -----

    def assign_task(self, worker_id: str, fetch_address: str) -> pb.MapTask | pb.ReduceTask | None:
        """Pick a task for an idle worker and start a new attempt of it, or return None if there is nothing to do.

        Jobs are served in submission order. Within a job, maps run before
        reduces (a reduce needs every map's output). In order of preference:
          1. an IDLE task
          2. an IN_PROGRESS task whose newest attempt has run longer than the
             task timeout (it may have crashed or be stuck)
          3. if the job uses backup tasks and its current phase is nearly
             done, a backup attempt of a task that has only one attempt
        Extra attempts always go to a different worker, and whichever attempt
        finishes first wins.
        """
        now = self._clock()
        with self._lock:
            for job in self._jobs.values():
                if job.is_finished:
                    continue
                phase_tasks = job.map_tasks if job.state == pb.JOB_STATE_MAPPING else job.reduce_tasks
                choice = self._pick_task(job, phase_tasks, worker_id, now)
                if choice is not None:
                    task, why = choice
                    return self._start_attempt(job, task, why, worker_id, fetch_address, now)
            return None

    def report_task_done(self, worker_id: str, attempt: pb.TaskAttempt, metrics: pb.TaskMetrics) -> bool:
        """Record a finished attempt. Returns False if the report was ignored (duplicate or stale)."""
        now = self._clock()
        with self._lock:
            job, task = self._find_task(attempt)

            def record_finish(outcome: str) -> None:
                self._record(
                    job,
                    "attempt_finished",
                    **self._attempt_fields(task, attempt.attempt_id, worker_id),
                    outcome=outcome,
                    run_seconds=round(metrics.run_seconds, 3),
                    records_in=metrics.records_in,
                    records_out=metrics.records_out,
                    bytes_written=metrics.bytes_written,
                    bytes_fetched=metrics.bytes_fetched,
                )

            # Check for a duplicate before checking whether the job is over: when a
            # backup wins a job's last task, the losing attempt reports after the
            # job has finished, and it is still a duplicate.
            if task.state == TaskState.DONE:
                job.duplicate_reports += 1
                finished_backup = self._running_attempt(task, attempt.attempt_id, worker_id)
                if finished_backup is not None:
                    del task.running_attempts[finished_backup.attempt_id]  # it's over, even though it lost
                log.info(
                    "%s: ignoring attempt %d of %s; attempt %d was already accepted",
                    job.job_id, attempt.attempt_id, task.name, task.accepted_attempt.attempt_id,
                )
                record_finish("duplicate")
                return False
            if job.is_finished:
                # Only a failed job can finish with this task undone.
                record_finish("after job finished")
                return False
            running = self._running_attempt(task, attempt.attempt_id, worker_id)
            if running is None:
                log.warning(
                    "%s: ignoring completion of unknown attempt %d of %s from %s",
                    job.job_id, attempt.attempt_id, task.name, worker_id,
                )
                record_finish("stale")
                return False

            state_before = job.state
            del task.running_attempts[running.attempt_id]
            task.accepted_attempt = running
            record_finish("accepted")
            log.info(
                "%s: %s done (attempt %d on %s, %.1fs)",
                job.job_id, task.name, running.attempt_id, worker_id, now - running.start_time,
            )
            if job.state != state_before:
                log.info("%s is now %s", job.job_id, pb.JobState.Name(job.state))
            if job.state == pb.JOB_STATE_SUCCEEDED:
                self._record(job, "job_finished", state="SUCCEEDED", failure_reason="")
            return True

    def report_task_failed(
        self,
        worker_id: str,
        attempt: pb.TaskAttempt,
        error: str,
        unfetchable_map_task_id: int | None,
    ) -> bool:
        """Record a failed attempt. Returns False if the report was ignored (stale).

        If a reducer could not fetch a map task's output, that map task is run
        again and the failure does not count against the reduce task. Any
        other failure counts against the task, and the job fails once a task
        has failed MAX_FAILURES_PER_TASK times.
        """
        with self._lock:
            job, task = self._find_task(attempt)

            def record_failure(outcome: str) -> None:
                self._record(
                    job,
                    "attempt_failed",
                    **self._attempt_fields(task, attempt.attempt_id, worker_id),
                    outcome=outcome,
                    error=error,
                    unfetchable_map_task_id=unfetchable_map_task_id,
                )

            if job.is_finished:
                record_failure("after job finished")
                return False
            running = self._running_attempt(task, attempt.attempt_id, worker_id)
            if running is None:
                log.warning(
                    "%s: ignoring failure of unknown attempt %d of %s from %s",
                    job.job_id, attempt.attempt_id, task.name, worker_id,
                )
                record_failure("stale")
                return False
            del task.running_attempts[running.attempt_id]

            if unfetchable_map_task_id is not None:
                record_failure("map output unfetchable")
                self._handle_unfetchable_map_output(job, task, running, unfetchable_map_task_id)
                return True
            if task.state == TaskState.DONE:
                # Another attempt already succeeded, so this failure changes nothing.
                log.info(
                    "%s: attempt %d of %s failed after the task was already done: %s",
                    job.job_id, running.attempt_id, task.name, error,
                )
                record_failure("after task done")
                return True

            record_failure("counted")
            task.failure_count += 1
            log.warning(
                "%s: attempt %d of %s failed on %s (failure %d of %d): %s",
                job.job_id, running.attempt_id, task.name, worker_id,
                task.failure_count, MAX_FAILURES_PER_TASK, error,
            )
            if task.failure_count >= MAX_FAILURES_PER_TASK:
                job.failure_reason = (
                    f"{task.name} failed {task.failure_count} times; last error: {error}"
                )
                log.error("%s failed: %s", job.job_id, job.failure_reason)
                self._record(job, "job_finished", state="FAILED", failure_reason=job.failure_reason)
            return True

    def handle_dead_worker(self, worker_id: str) -> None:
        """Forget everything a dead worker was doing or holding.

        Its running attempts are dropped, so those tasks become IDLE unless
        another attempt is still running. Its completed map tasks are run
        again, because their output was on its local disk; this only matters
        for jobs whose reduces are not all done yet. Completed reduce tasks
        are kept: their output is already in shared storage.
        """
        with self._lock:
            for job in self._jobs.values():
                if job.is_finished:
                    continue
                for task in job.map_tasks + job.reduce_tasks:
                    lost_attempt_ids = [
                        attempt_id
                        for attempt_id, running in task.running_attempts.items()
                        if running.worker_id == worker_id
                    ]
                    for attempt_id in lost_attempt_ids:
                        del task.running_attempts[attempt_id]
                        log.info("%s: attempt %d of %s lost with %s", job.job_id, attempt_id, task.name, worker_id)
                        self._record(job, "attempt_lost", **self._attempt_fields(task, attempt_id, worker_id))
                for task in job.map_tasks:
                    if task.accepted_attempt is not None and task.accepted_attempt.worker_id == worker_id:
                        lost_attempt_id = task.accepted_attempt.attempt_id
                        task.accepted_attempt = None
                        log.info("%s: re-running %s; its output was on %s", job.job_id, task.name, worker_id)
                        self._record(
                            job,
                            "map_output_lost",
                            **self._attempt_fields(task, lost_attempt_id, worker_id),
                            reason="worker died",
                        )

    def duplicate_reports(self, job_id: str) -> int:
        with self._lock:
            return self._get_job(job_id).duplicate_reports

    # ----- helpers (call with self._lock held) -----

    def _get_job(self, job_id: str) -> Job:
        if job_id not in self._jobs:
            raise UnknownJobError(job_id)
        return self._jobs[job_id]

    def _find_task(self, attempt: pb.TaskAttempt) -> tuple[Job, Task]:
        job = self._get_job(attempt.job_id)
        if attempt.kind == pb.TASK_KIND_MAP:
            tasks = job.map_tasks
        elif attempt.kind == pb.TASK_KIND_REDUCE:
            tasks = job.reduce_tasks
        else:
            raise InvalidReportError(f"attempt has no task kind: {attempt}")
        if not 0 <= attempt.task_id < len(tasks):
            raise InvalidReportError(f"{attempt.job_id} has no task {attempt.task_id} of kind {attempt.kind}")
        return job, tasks[attempt.task_id]

    def _record(self, job: Job, event_name: str, **fields) -> None:
        """Add one event to the job's event log, timestamped relative to submission."""
        event = {
            "event": event_name,
            "job_id": job.job_id,
            "t": round(self._clock() - job.submit_time, 3),
            **fields,
        }
        try:
            self._events.record(job.spec.output_path, event)
        except OSError as error:
            # The event log is for evaluation, not correctness. By the time an
            # event is recorded, the scheduling decision it describes has already
            # been made, so failing here would only turn a correct RPC into an
            # error. Report the loss loudly instead; any plot built from this
            # job's log is then incomplete.
            self._dropped_events += 1
            log.error(
                "%s: could not write %s event to %s (%d events dropped so far): %s",
                job.job_id, event_name, job.spec.output_path, self._dropped_events, error,
            )

    @staticmethod
    def _attempt_fields(task: Task, attempt_id: int, worker_id: str) -> dict:
        return {"kind": task.kind_name, "task_id": task.task_id, "attempt_id": attempt_id, "worker_id": worker_id}

    @staticmethod
    def _running_attempt(task: Task, attempt_id: int, worker_id: str) -> Attempt | None:
        """The running attempt with this ID, if it was assigned to this worker."""
        running = task.running_attempts.get(attempt_id)
        if running is None or running.worker_id != worker_id:
            return None
        return running

    def _pick_task(self, job: Job, phase_tasks: list[Task], worker_id: str, now: float) -> tuple[Task, str] | None:
        """The task to start an attempt of, and why: "idle", "timeout" or "backup"."""
        for task in phase_tasks:
            if task.state == TaskState.IDLE:
                return task, "idle"

        # Every unfinished task in this phase is running. A second attempt on
        # a worker that is already running the task would not help.
        running_elsewhere = [
            task
            for task in phase_tasks
            if task.state == TaskState.IN_PROGRESS
            and all(running.worker_id != worker_id for running in task.running_attempts.values())
        ]
        for task in running_elsewhere:
            newest_start = max(running.start_time for running in task.running_attempts.values())
            if now - newest_start > self._task_timeout_seconds:
                return task, "timeout"

        if job.spec.use_backup_tasks:
            unfinished = sum(task.state != TaskState.DONE for task in phase_tasks)
            backup_threshold = max(1, math.ceil(BACKUP_PHASE_FRACTION * len(phase_tasks)))
            if unfinished <= backup_threshold:
                not_yet_backed_up = [task for task in running_elsewhere if len(task.running_attempts) == 1]
                if not_yet_backed_up:
                    # Back up the task that has been running longest: it is the likeliest straggler.
                    oldest = min(
                        not_yet_backed_up,
                        key=lambda task: next(iter(task.running_attempts.values())).start_time,
                    )
                    return oldest, "backup"
        return None

    def _start_attempt(
        self, job: Job, task: Task, why: str, worker_id: str, fetch_address: str, now: float
    ) -> pb.MapTask | pb.ReduceTask:
        attempt_id = task.next_attempt_id
        task.next_attempt_id += 1
        map_attempt_ids = ()
        if task.kind == pb.TASK_KIND_REDUCE:
            map_attempt_ids = tuple(map_task.accepted_attempt.attempt_id for map_task in job.map_tasks)
        task.running_attempts[attempt_id] = Attempt(
            attempt_id, worker_id, fetch_address, start_time=now, map_attempt_ids=map_attempt_ids
        )
        if why == "idle":
            # A task is idle the first time, or again after a failure, a lost
            # worker, or lost map output.
            reason = "first" if attempt_id == 0 else "rerun"
        else:
            reason = why  # "timeout" or "backup"
        explanation = {
            "first": "",
            "rerun": " (running it again)",
            "timeout": " (earlier attempt timed out)",
            "backup": " (backup task near the end of the phase)",
        }[reason]
        log.info("%s: assigning attempt %d of %s to %s%s", job.job_id, attempt_id, task.name, worker_id, explanation)
        self._record(job, "attempt_started", **self._attempt_fields(task, attempt_id, worker_id), reason=reason)

        attempt = pb.TaskAttempt(job_id=job.job_id, kind=task.kind, task_id=task.task_id, attempt_id=attempt_id)
        if task.kind == pb.TASK_KIND_MAP:
            return pb.MapTask(
                attempt=attempt,
                splits=job.map_splits[task.task_id],
                num_reduce_tasks=len(job.reduce_tasks),
                job_code=job.spec.job_code,
                use_combiner=job.spec.use_combiner,
            )
        return pb.ReduceTask(
            attempt=attempt,
            map_outputs=[
                pb.MapOutputLocation(
                    map_task_id=map_task.task_id,
                    attempt_id=map_task.accepted_attempt.attempt_id,
                    fetch_address=map_task.accepted_attempt.fetch_address,
                )
                for map_task in job.map_tasks
            ],
            output_path=job.spec.output_path,
            job_code=job.spec.job_code,
        )

    def _handle_unfetchable_map_output(
        self, job: Job, reduce_task: Task, reduce_attempt: Attempt, map_task_id: int
    ) -> None:
        if not 0 <= map_task_id < len(job.map_tasks):
            raise InvalidReportError(f"{job.job_id} has no map task {map_task_id}")
        map_task = job.map_tasks[map_task_id]
        accepted = map_task.accepted_attempt
        # Only re-run the map if the reducer was told to fetch its current
        # output. Otherwise the map has already been re-run since this reduce
        # attempt started, and the reducer just had an old address.
        if accepted is not None and accepted.attempt_id == reduce_attempt.map_attempt_ids[map_task_id]:
            map_task.accepted_attempt = None
            log.warning(
                "%s: %s could not fetch %s output from %s; re-running %s",
                job.job_id, reduce_task.name, map_task.name, accepted.worker_id, map_task.name,
            )
            self._record(
                job,
                "map_output_lost",
                **self._attempt_fields(map_task, accepted.attempt_id, accepted.worker_id),
                reason="unfetchable",
            )
        else:
            log.info(
                "%s: %s could not fetch %s output, which has already been re-run; retrying %s",
                job.job_id, reduce_task.name, map_task.name, reduce_task.name,
            )
