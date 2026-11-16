"""Run and plot the three-by-two word-count combiner comparison from design.md.

This script intentionally leaves worker failures, CPU stragglers, and backup
tasks out of the experiment.  The only difference between its two groups is
whether a mapper runs the word-count ``combine`` function.
"""

import argparse
import csv
import statistics
import sys
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterator

import grpc
import matplotlib

matplotlib.use("Agg")
from matplotlib import pyplot as plt

from boss.events import read_events
from client.cli import (
    DEFAULT_BOSS_ADDRESS,
    DEFAULT_SHARED_ROOT,
    RPC_TIMEOUT_SECONDS,
    ClientError,
    build_job_spec,
    host_shared_path,
    submit_job,
    wait_for_job,
)
from protos import mapreduce_pb2 as pb
from protos import mapreduce_pb2_grpc as pb_grpc


DEFAULT_INPUT_PATH = "/input/books"
DEFAULT_JOB_PATH = Path("examples/wordcount.py")
DEFAULT_MAP_TASKS = 8
DEFAULT_REDUCE_TASKS = 4
DEFAULT_RESULTS_PATH = Path("eval/results.csv")
DEFAULT_PLOT_PATH = Path("eval/plots/combiner.png")
REPEAT_COUNT = 3
EXPERIMENT_NAME = "combiner"
CONFIGURATION_OFF = "without_combiner"
CONFIGURATION_ON = "with_combiner"
RESULT_FIELDNAMES = (
    "experiment",
    "run_id",
    "configuration",
    "run_number",
    "num_map_tasks",
    "num_reduce_tasks",
    "input_path",
    "output_path",
    "job_id",
    "job_seconds",
    "accepted_map_bytes",
)
OUTPUT_CHUNK_BYTES = 1024 * 1024


class EvaluationError(RuntimeError):
    """An evaluation input, event log, or completed result is unusable."""


@dataclass(frozen=True)
class CombinerResult:
    """One successful job represented by the fields persisted to results.csv."""

    run_id: str
    configuration: str
    run_number: int
    num_map_tasks: int
    num_reduce_tasks: int
    input_path: str
    output_path: str
    job_id: str
    job_seconds: float
    accepted_map_bytes: int

    def csv_row(self) -> dict[str, str | int | float]:
        """Return a row with the shared evaluation CSV schema."""
        return {
            "experiment": EXPERIMENT_NAME,
            "run_id": self.run_id,
            "configuration": self.configuration,
            "run_number": self.run_number,
            "num_map_tasks": self.num_map_tasks,
            "num_reduce_tasks": self.num_reduce_tasks,
            "input_path": self.input_path,
            "output_path": self.output_path,
            "job_id": self.job_id,
            "job_seconds": self.job_seconds,
            "accepted_map_bytes": self.accepted_map_bytes,
        }


def positive_int(value: str) -> int:
    """Parse one strictly positive command-line task count."""
    try:
        number = int(value)
    except ValueError as error:
        raise argparse.ArgumentTypeError(f"expected an integer, got {value!r}") from error
    if number < 1:
        raise argparse.ArgumentTypeError(f"must be at least 1, got {number}")
    return number


def default_run_id() -> str:
    """Generate a unique, readable identifier for an evaluation invocation."""
    return datetime.now(timezone.utc).strftime("combiner-%Y%m%dT%H%M%S-%fZ")


def accepted_map_metrics(events: list[dict], expected_map_tasks: int) -> tuple[float, int]:
    """Return successful job duration and bytes from exactly one accepted map per task."""
    if not events:
        raise EvaluationError("event log is empty")
    if not any(event.get("event") == "job_submitted" for event in events):
        raise EvaluationError("event log has no job_submitted event")

    finished = [event for event in events if event.get("event") == "job_finished"]
    if len(finished) != 1:
        raise EvaluationError(f"event log must contain one job_finished event, found {len(finished)}")
    if finished[0].get("state") != "SUCCEEDED":
        reason = finished[0].get("failure_reason", "no reason recorded")
        raise EvaluationError(f"job did not succeed: {reason}")

    try:
        job_seconds = float(finished[0]["t"])
    except (KeyError, TypeError, ValueError) as error:
        raise EvaluationError("successful job_finished event has no numeric t") from error
    if job_seconds < 0:
        raise EvaluationError("successful job_finished event has a negative t")

    accepted_maps = [
        event
        for event in events
        if event.get("event") == "attempt_finished"
        and event.get("kind") == "map"
        and event.get("outcome") == "accepted"
    ]
    try:
        accepted_task_ids = [int(event["task_id"]) for event in accepted_maps]
    except (KeyError, TypeError, ValueError) as error:
        raise EvaluationError("accepted map finish has no integer task_id") from error
    expected_task_ids = list(range(expected_map_tasks))
    if sorted(accepted_task_ids) != expected_task_ids:
        raise EvaluationError(
            "event log is incomplete: expected one accepted map finish for each "
            f"task {expected_task_ids}, found {accepted_task_ids}"
        )

    try:
        bytes_written = sum(int(event["bytes_written"]) for event in accepted_maps)
    except (KeyError, TypeError, ValueError) as error:
        raise EvaluationError("accepted map finish has no integer bytes_written") from error
    if bytes_written < 0:
        raise EvaluationError("accepted map bytes cannot be negative")
    return job_seconds, bytes_written


def final_output_files(output_directory: Path, expected_reduce_tasks: int) -> list[Path]:
    """Return all final reducer parts, rejecting missing or partial output."""
    if not output_directory.is_dir():
        raise EvaluationError(f"output directory is missing: {output_directory}")
    try:
        parts = sorted(
            candidate
            for candidate in output_directory.iterdir()
            if candidate.is_file()
            and candidate.name.startswith("part-r-")
            and candidate.name.removeprefix("part-r-").isdigit()
            and len(candidate.name.removeprefix("part-r-")) == 5
        )
    except OSError as error:
        raise EvaluationError(f"could not list output directory {output_directory}: {error}") from error
    expected_names = [f"part-r-{task_id:05d}" for task_id in range(expected_reduce_tasks)]
    if [part.name for part in parts] != expected_names:
        raise EvaluationError(
            f"expected final reducer files {expected_names}, found {[part.name for part in parts]}"
        )
    return parts


def outputs_match(
    reference_directory: Path,
    candidate_directory: Path,
    expected_reduce_tasks: int,
) -> bool:
    """Compare two reducer-output directories byte for byte without loading them all."""
    reference_parts = final_output_files(reference_directory, expected_reduce_tasks)
    candidate_parts = final_output_files(candidate_directory, expected_reduce_tasks)
    for reference, candidate in zip(reference_parts, candidate_parts, strict=True):
        try:
            with reference.open("rb") as reference_file, candidate.open("rb") as candidate_file:
                while True:
                    reference_chunk = reference_file.read(OUTPUT_CHUNK_BYTES)
                    candidate_chunk = candidate_file.read(OUTPUT_CHUNK_BYTES)
                    if reference_chunk != candidate_chunk:
                        return False
                    if not reference_chunk:
                        break
        except OSError as error:
            raise EvaluationError(f"could not compare {reference} and {candidate}: {error}") from error
    return True


def append_result(results_path: Path, result: CombinerResult) -> None:
    """Append one completed experiment run, creating the shared CSV header once."""
    try:
        results_path.parent.mkdir(parents=True, exist_ok=True)
        write_header = not results_path.exists() or results_path.stat().st_size == 0
        if not write_header:
            with results_path.open(newline="", encoding="utf-8") as existing_file:
                header = next(csv.reader(existing_file), None)
            if header is None or not set(RESULT_FIELDNAMES).issubset(header):
                raise EvaluationError(f"results CSV has no complete shared header: {results_path}")
        with results_path.open("a", newline="", encoding="utf-8") as results_file:
            writer = csv.DictWriter(results_file, fieldnames=RESULT_FIELDNAMES)
            if write_header:
                writer.writeheader()
            writer.writerow(result.csv_row())
    except OSError as error:
        raise EvaluationError(f"could not append to {results_path}: {error}") from error


def read_combiner_results(results_path: Path, run_id: str) -> list[CombinerResult]:
    """Load and validate the six design-required rows for one combiner evaluation."""
    try:
        with results_path.open(newline="", encoding="utf-8") as results_file:
            reader = csv.DictReader(results_file)
            if reader.fieldnames is None or not set(RESULT_FIELDNAMES).issubset(reader.fieldnames):
                raise EvaluationError(f"results CSV has no complete shared header: {results_path}")
            matching_rows = [
                row
                for row in reader
                if row["experiment"] == EXPERIMENT_NAME and row["run_id"] == run_id
            ]
    except OSError as error:
        raise EvaluationError(f"could not read results CSV {results_path}: {error}") from error

    results = []
    for row in matching_rows:
        try:
            result = CombinerResult(
                run_id=row["run_id"],
                configuration=row["configuration"],
                run_number=int(row["run_number"]),
                num_map_tasks=int(row["num_map_tasks"]),
                num_reduce_tasks=int(row["num_reduce_tasks"]),
                input_path=row["input_path"],
                output_path=row["output_path"],
                job_id=row["job_id"],
                job_seconds=float(row["job_seconds"]),
                accepted_map_bytes=int(row["accepted_map_bytes"]),
            )
        except (KeyError, TypeError, ValueError) as error:
            raise EvaluationError(f"invalid combiner row in {results_path}: {row}") from error
        if result.configuration not in (CONFIGURATION_OFF, CONFIGURATION_ON):
            raise EvaluationError(f"unknown combiner configuration: {result.configuration!r}")
        if result.run_number not in range(1, REPEAT_COUNT + 1):
            raise EvaluationError(f"invalid run number: {result.run_number}")
        if result.job_seconds < 0 or result.accepted_map_bytes < 0:
            raise EvaluationError("result measurements cannot be negative")
        results.append(result)

    if len(results) != REPEAT_COUNT * 2:
        raise EvaluationError(
            f"expected {REPEAT_COUNT * 2} combiner rows for {run_id!r}, found {len(results)}"
        )
    for configuration in (CONFIGURATION_OFF, CONFIGURATION_ON):
        run_numbers = sorted(result.run_number for result in results if result.configuration == configuration)
        if run_numbers != list(range(1, REPEAT_COUNT + 1)):
            raise EvaluationError(
                f"{configuration} must have run numbers 1 through {REPEAT_COUNT}, found {run_numbers}"
            )
    return results


def summarize(results: list[CombinerResult], configuration: str, field: str) -> tuple[float, float, float]:
    """Return the median and min/max error-bar endpoints for one measurement."""
    values = [getattr(result, field) for result in results if result.configuration == configuration]
    if len(values) != REPEAT_COUNT:
        raise EvaluationError(f"expected {REPEAT_COUNT} {configuration} values for {field}")
    return float(statistics.median(values)), float(min(values)), float(max(values))


def plot_combiner_results(results: list[CombinerResult], output_path: Path) -> None:
    """Render design plot 2: shuffle bytes and job time, off versus on."""
    configurations = (CONFIGURATION_OFF, CONFIGURATION_ON)
    labels = ("Combiner off", "Combiner on")
    bytes_summaries = [summarize(results, configuration, "accepted_map_bytes") for configuration in configurations]
    time_summaries = [summarize(results, configuration, "job_seconds") for configuration in configurations]

    figure, axes = plt.subplots(ncols=2, figsize=(10, 4.8), layout="constrained")
    for axis, title, ylabel, summaries in (
        (axes[0], "Intermediate data written", "Accepted map bytes", bytes_summaries),
        (axes[1], "Job time", "Seconds", time_summaries),
    ):
        medians = [summary[0] for summary in summaries]
        lower_errors = [summary[0] - summary[1] for summary in summaries]
        upper_errors = [summary[2] - summary[0] for summary in summaries]
        axis.bar(labels, medians, yerr=[lower_errors, upper_errors], capsize=6, color=("#6c757d", "#0077b6"))
        axis.set_title(title)
        axis.set_ylabel(ylabel)
        axis.set_axisbelow(True)
        axis.grid(axis="y", alpha=0.3)

    figure.suptitle("Word count with and without a map-side combiner")
    try:
        output_path.parent.mkdir(parents=True, exist_ok=True)
        figure.savefig(output_path, dpi=160)
    except OSError as error:
        raise EvaluationError(f"could not write plot {output_path}: {error}") from error
    finally:
        plt.close(figure)


@contextmanager
def boss_stub(address: str) -> Iterator[pb_grpc.BossStub]:
    """Connect once to the host-published boss for all six submissions."""
    channel = grpc.insecure_channel(address)
    try:
        grpc.channel_ready_future(channel).result(timeout=RPC_TIMEOUT_SECONDS)
    except grpc.FutureTimeoutError as error:
        channel.close()
        raise EvaluationError(f"could not reach the boss at {address}") from error
    try:
        yield pb_grpc.BossStub(channel)
    finally:
        channel.close()


def run_comparison(
    *,
    run_id: str,
    input_path: str,
    job_path: Path,
    num_map_tasks: int,
    num_reduce_tasks: int,
    shared_root: Path,
    results_path: Path,
    boss_address: str,
) -> list[CombinerResult]:
    """Run three jobs per configuration and append their validated measurements."""
    reference_output: Path | None = None
    results = []
    with boss_stub(boss_address) as stub:
        for configuration, use_combiner in (
            (CONFIGURATION_OFF, False),
            (CONFIGURATION_ON, True),
        ):
            for run_number in range(1, REPEAT_COUNT + 1):
                output_path = f"/output/{run_id}/{configuration}-{run_number}"
                try:
                    spec = build_job_spec(
                        input_path,
                        output_path,
                        job_path,
                        num_map_tasks,
                        num_reduce_tasks,
                        use_combiner,
                        False,
                    )
                    job_id = submit_job(stub, spec)
                    status = wait_for_job(stub, job_id)
                except ClientError as error:
                    raise EvaluationError(str(error)) from error
                if status.state != pb.JOB_STATE_SUCCEEDED:
                    raise EvaluationError(f"{job_id} failed: {status.failure_reason}")

                output_directory = host_shared_path(output_path, shared_root)
                try:
                    events = read_events(str(output_directory))
                except (OSError, ValueError) as error:
                    raise EvaluationError(f"could not read event log for {job_id}: {error}") from error
                job_seconds, accepted_map_bytes = accepted_map_metrics(events, num_map_tasks)
                if reference_output is None:
                    reference_output = output_directory
                elif not outputs_match(reference_output, output_directory, num_reduce_tasks):
                    raise EvaluationError(
                        f"final output for {job_id} differs from the first no-combiner run"
                    )

                result = CombinerResult(
                    run_id=run_id,
                    configuration=configuration,
                    run_number=run_number,
                    num_map_tasks=num_map_tasks,
                    num_reduce_tasks=num_reduce_tasks,
                    input_path=input_path,
                    output_path=output_path,
                    job_id=job_id,
                    job_seconds=job_seconds,
                    accepted_map_bytes=accepted_map_bytes,
                )
                append_result(results_path, result)
                results.append(result)
                print(
                    f"{configuration} run {run_number}/{REPEAT_COUNT}: {job_id} "
                    f"({job_seconds:.3f}s, {accepted_map_bytes} accepted map bytes)"
                )
    return results


def build_parser() -> argparse.ArgumentParser:
    """Build distinct commands for the cluster run and PNG generation."""
    parser = argparse.ArgumentParser(description="Evaluate the word-count combiner")
    commands = parser.add_subparsers(dest="command", required=True)

    run_parser = commands.add_parser("run", help="run three word-count jobs with and without the combiner")
    run_parser.add_argument("--run-id", default=default_run_id(), help="unique name used below /output")
    run_parser.add_argument("--input", default=DEFAULT_INPUT_PATH, help="shared input path (default: /input/books)")
    run_parser.add_argument("--job", type=Path, default=DEFAULT_JOB_PATH, help="word-count job file")
    run_parser.add_argument("--maps", type=positive_int, default=DEFAULT_MAP_TASKS)
    run_parser.add_argument("--reduces", type=positive_int, default=DEFAULT_REDUCE_TASKS)
    run_parser.add_argument("--shared-root", type=Path, default=DEFAULT_SHARED_ROOT)
    run_parser.add_argument("--results", type=Path, default=DEFAULT_RESULTS_PATH)
    run_parser.add_argument("--boss", default=DEFAULT_BOSS_ADDRESS)

    plot_parser = commands.add_parser("plot", help="generate the combiner PNG from one six-run result set")
    plot_parser.add_argument("--run-id", required=True, help="the completed combiner evaluation to plot")
    plot_parser.add_argument("--results", type=Path, default=DEFAULT_RESULTS_PATH)
    plot_parser.add_argument("--output", type=Path, default=DEFAULT_PLOT_PATH)
    return parser


def main(argv: list[str] | None = None) -> int:
    """Run the selected evaluation command and return a shell-friendly status."""
    args = build_parser().parse_args(argv)
    try:
        if args.command == "run":
            run_comparison(
                run_id=args.run_id,
                input_path=args.input,
                job_path=args.job,
                num_map_tasks=args.maps,
                num_reduce_tasks=args.reduces,
                shared_root=args.shared_root,
                results_path=args.results,
                boss_address=args.boss,
            )
            print(f"Saved six rows for {args.run_id!r} in {args.results}")
        else:
            results = read_combiner_results(args.results, args.run_id)
            plot_combiner_results(results, args.output)
            print(f"Saved {args.output}")
    except EvaluationError as error:
        print(f"error: {error}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
