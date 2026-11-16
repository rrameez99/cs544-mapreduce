"""Running experiments on the real cluster: Docker Compose control, submitting a
job and waiting for it, and keeping each run's event log in the repo.

Run experiments from the repo root on the VM, after building the image and
creating the dataset (see README.md). Each run's _events.jsonl is copied to
eval/results/<experiment>/, so the plots can be redrawn later from the repo
alone, without rerunning anything.
"""

import collections
import functools
import json
import shutil
import subprocess
import time
from collections.abc import Callable
from dataclasses import asdict, dataclass, field
from pathlib import Path

import grpc

from boss.events import EVENT_LOG_FILE_NAME
from eval.analysis import job_seconds, load_event_file
from protos import mapreduce_pb2 as pb
from protos import mapreduce_pb2_grpc as pb_grpc

REPO_ROOT = Path(__file__).resolve().parent.parent
SHARED_ROOT = REPO_ROOT / "shared"  # mounted at /shared in every container
RESULTS_ROOT = REPO_ROOT / "eval" / "results"
PLOTS_ROOT = REPO_ROOT / "eval" / "plots"

BOSS_ADDRESS = "localhost:5440"
ALL_WORKERS = ["worker-1", "worker-2", "worker-3", "worker-4"]
STRAGGLER_COMPOSE_FILE = "docker-compose.straggler.yml"
DEFAULT_INPUT = "input/books"  # under shared storage; created by the dataset script
DEFAULT_JOB_FILE = REPO_ROOT / "examples" / "wordcount.py"

CLUSTER_START_TIMEOUT_SECONDS = 60.0
JOB_TIMEOUT_SECONDS = 900.0
POLL_INTERVAL_SECONDS = 0.2
RPC_TIMEOUT_SECONDS = 10.0


class ExperimentError(Exception):
    """The experiment cannot run or did not produce a usable result."""


@dataclass
class RunResult:
    experiment: str
    label: str  # the configuration, e.g. "4 workers" or "kill during map"
    run: int  # repetition number, from 1
    job_id: str
    event_log: str  # path of the copied event log, relative to the experiment's results folder
    job_seconds: float
    notes: dict = field(default_factory=dict)  # experiment-specific, e.g. when a worker was killed


# ----- cluster control -----


def _compose(*args: str, straggler: bool = False) -> subprocess.CompletedProcess:
    files = ["-f", "docker-compose.yml"]
    if straggler:
        files += ["-f", STRAGGLER_COMPOSE_FILE]
    command = ["docker", "compose", *files, *args]
    completed = subprocess.run(command, cwd=REPO_ROOT, capture_output=True, text=True)
    if completed.returncode != 0:
        raise ExperimentError(f"`{' '.join(command)}` failed:\n{completed.stderr.strip()}")
    return completed


def start_cluster(num_workers: int = len(ALL_WORKERS), straggler: bool = False) -> None:
    """Start a fresh cluster (boss plus the first num_workers workers) and wait until they all registered."""
    if not 1 <= num_workers <= len(ALL_WORKERS):
        raise ExperimentError(f"num_workers must be 1-{len(ALL_WORKERS)}, got {num_workers}")
    _compose("down")
    _compose("up", "-d", "boss", *ALL_WORKERS[:num_workers], straggler=straggler)
    wait_for_live_workers(num_workers)


def stop_cluster() -> None:
    _compose("down")


def kill_worker(worker_id: str) -> None:
    _compose("kill", worker_id)


def live_workers_from_boss_log(boss_log: str) -> list[str]:
    """The workers named on the boss's most recent "live workers:" log line."""
    lines = [line for line in boss_log.splitlines() if "live workers:" in line]
    if not lines:
        return []
    names = lines[-1].split("live workers:", 1)[1].split(",")
    return [name.strip() for name in names if name.strip().startswith("worker-")]


def wait_for_live_workers(num_workers: int) -> None:
    deadline = time.monotonic() + CLUSTER_START_TIMEOUT_SECONDS
    while time.monotonic() < deadline:
        live = live_workers_from_boss_log(_compose("logs", "boss", "--no-log-prefix").stdout)
        if len(live) == num_workers:
            return
        time.sleep(0.5)
    raise ExperimentError(f"{num_workers} workers did not all register within {CLUSTER_START_TIMEOUT_SECONDS:.0f}s")


# ----- running one job -----

# Called on every status poll with (latest status, seconds since submission).
# Experiments use it to kill a worker at the right moment; it returns notes to keep.
DuringJob = Callable[[pb.JobStatus, float], dict]


def run_job(
    experiment: str,
    label: str,
    run: int,
    num_map_tasks: int,
    num_reduce_tasks: int,
    use_combiner: bool = False,
    use_backup_tasks: bool = False,
    during_job: DuringJob | None = None,
) -> RunResult:
    """Submit one job, wait for it to succeed, and keep its event log under eval/results/."""
    input_dir = SHARED_ROOT / DEFAULT_INPUT
    if not input_dir.is_dir():
        raise ExperimentError(f"no dataset at {input_dir}; create it first (see README.md)")
    if not DEFAULT_JOB_FILE.is_file():
        raise ExperimentError(f"no job file at {DEFAULT_JOB_FILE}")

    # A new output directory per run, so runs never collide and every event log is kept.
    output_name = f"output/eval/{experiment}/{_slug(label)}-run{run}-{time.strftime('%H%M%S')}"
    spec = pb.JobSpec(
        input_path=f"/shared/{DEFAULT_INPUT}",
        output_path=f"/shared/{output_name}",
        job_file_name=DEFAULT_JOB_FILE.name,
        job_code=DEFAULT_JOB_FILE.read_bytes(),
        num_map_tasks=num_map_tasks,
        num_reduce_tasks=num_reduce_tasks,
        use_combiner=use_combiner,
        use_backup_tasks=use_backup_tasks,
    )

    channel = grpc.insecure_channel(BOSS_ADDRESS)
    try:
        boss = pb_grpc.BossStub(channel)
        submitted_at = time.monotonic()
        job_id = boss.SubmitJob(pb.SubmitJobRequest(spec=spec), timeout=RPC_TIMEOUT_SECONDS).job_id
        notes: dict = {}
        while True:
            status = boss.GetJobStatus(pb.GetJobStatusRequest(job_id=job_id), timeout=RPC_TIMEOUT_SECONDS)
            elapsed = time.monotonic() - submitted_at
            if status.state == pb.JOB_STATE_FAILED:
                raise ExperimentError(f"{experiment} {label} run {run}: {job_id} failed: {status.failure_reason}")
            if status.state == pb.JOB_STATE_SUCCEEDED:
                break
            if elapsed > JOB_TIMEOUT_SECONDS:
                raise ExperimentError(f"{experiment} {label} run {run}: {job_id} still running after {elapsed:.0f}s")
            if during_job is not None:
                notes.update(during_job(status, elapsed))
            time.sleep(POLL_INTERVAL_SECONDS)
    finally:
        channel.close()

    check_word_count_output(SHARED_ROOT / output_name)
    kept_log = RESULTS_ROOT / experiment / f"{_slug(label)}-run{run}.jsonl"
    kept_log.parent.mkdir(parents=True, exist_ok=True)
    shutil.copyfile(SHARED_ROOT / output_name / EVENT_LOG_FILE_NAME, kept_log)
    result = RunResult(
        experiment=experiment,
        label=label,
        run=run,
        job_id=job_id,
        event_log=kept_log.name,
        job_seconds=job_seconds(load_event_file(kept_log)),
        notes=notes,
    )
    print(f"{experiment} | {label} | run {run}: {result.job_seconds:.1f}s, output correct {notes or ''}", flush=True)
    return result


# ----- checking the output -----


@functools.cache
def reference_word_count() -> dict[str, int]:
    """Word counts of the dataset computed in one process, the answer every run must match."""
    counts: collections.Counter = collections.Counter()
    for path in sorted((SHARED_ROOT / DEFAULT_INPUT).iterdir()):
        with path.open(encoding="utf-8") as input_file:
            for line in input_file:
                counts.update(line.split())
    return dict(counts)


def check_word_count_output(output_dir: Path) -> None:
    """Fail the experiment unless the job's part-r-* files hold exactly the reference counts."""
    produced: dict[str, int] = {}
    for part_file in sorted(output_dir.glob("part-r-*")):
        with part_file.open(encoding="utf-8") as output_file:
            for line in output_file:
                word, count = line.rstrip("\n").split("\t")
                if word in produced:
                    raise ExperimentError(f"{output_dir}: {word!r} appears in more than one output line")
                produced[word] = int(count)
    if produced != reference_word_count():
        raise ExperimentError(f"{output_dir}: output does not match the single-process word count")


# ----- results on disk -----


def save_results(experiment: str, results: list[RunResult]) -> Path:
    path = RESULTS_ROOT / experiment / "runs.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps([asdict(result) for result in results], indent=2) + "\n", encoding="utf-8")
    return path


def load_results(experiment: str, results_root: Path = RESULTS_ROOT) -> list[RunResult]:
    path = results_root / experiment / "runs.json"
    if not path.is_file():
        raise ExperimentError(f"no results at {path}; run the experiment first")
    return [RunResult(**fields) for fields in json.loads(path.read_text(encoding="utf-8"))]


def load_events(result: RunResult, results_root: Path = RESULTS_ROOT) -> list[dict]:
    return load_event_file(results_root / result.experiment / result.event_log)


def _slug(label: str) -> str:
    return "".join(character if character.isalnum() else "-" for character in label.lower()).strip("-")
