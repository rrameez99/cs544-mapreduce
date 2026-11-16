"""Plots 3 and 4: what happens when a worker dies mid-job.

We kill worker-2 (docker compose kill, like pulling the plug) either during the
map phase or during the reduce phase, and compare with runs where nothing fails.

  - Plot 3 (failures_timeline.png): every task attempt as a bar on the worker
    that ran it. You can see worker-2's work being lost, and other workers
    re-running it, including finished map tasks whose output was on worker-2's
    disk (paper §3.3).
  - Plot 4 (failures_cost.png): how much a failure slows the job down.

    python -m eval.failures run [--repeats 3]   # on the VM
    python -m eval.failures plot
"""

import argparse
from pathlib import Path

from matplotlib.patches import Patch

from eval import style
from eval.analysis import attempt_intervals, extra_attempts, spread
from eval.harness import (
    ALL_WORKERS,
    PLOTS_ROOT,
    ExperimentError,
    RESULTS_ROOT,
    RunResult,
    kill_worker,
    load_events,
    load_results,
    run_job,
    save_results,
    start_cluster,
    stop_cluster,
)
from protos import mapreduce_pb2 as pb

EXPERIMENT = "failures"
NUM_MAP_TASKS = 16
NUM_REDUCE_TASKS = 4
VICTIM = "worker-2"
NO_FAILURE = "no failure"
KILL_DURING_MAP = "killed during map"
KILL_DURING_REDUCE = "killed during reduce"
LABELS = [NO_FAILURE, KILL_DURING_MAP, KILL_DURING_REDUCE]


def killer(when: str):
    """A during_job callback that kills VICTIM once, at the chosen point of the job."""
    killed = False

    def during_job(status: pb.JobStatus, elapsed: float) -> dict:
        nonlocal killed
        if killed:
            return {}
        in_map_phase = status.state == pb.JOB_STATE_MAPPING and status.map_tasks_done >= status.num_map_tasks // 4
        in_reduce_phase = status.state == pb.JOB_STATE_REDUCING
        if (when == KILL_DURING_MAP and in_map_phase) or (when == KILL_DURING_REDUCE and in_reduce_phase):
            kill_worker(VICTIM)
            killed = True
            return {"killed_worker": VICTIM, "kill_t": round(elapsed, 3)}
        return {}

    return during_job


def run(repeats: int) -> None:
    results = []
    for label in LABELS:
        for repetition in range(1, repeats + 1):
            start_cluster()
            during_job = None if label == NO_FAILURE else killer(label)
            result = run_job(EXPERIMENT, label, repetition, NUM_MAP_TASKS, NUM_REDUCE_TASKS, during_job=during_job)
            if label != NO_FAILURE and "kill_t" not in result.notes:
                raise ExperimentError(f"{label} run {repetition}: the job finished before {VICTIM} could be killed")
            results.append(result)
    stop_cluster()
    print(f"saved {save_results(EXPERIMENT, results)}")


# ----- plot 3: timeline -----


def _draw_timeline(axis, result: RunResult, results_root: Path) -> None:
    events = load_events(result, results_root)
    lost_outputs = {(e["task_id"], e["attempt_id"]) for e in events if e["event"] == "map_output_lost"}
    rows = {worker: index for index, worker in enumerate(reversed(ALL_WORKERS))}
    bar_height = 0.6
    for interval in attempt_intervals(events):
        wasted = interval.outcome != "accepted" or (
            interval.kind == "map" and (interval.task_id, interval.attempt_id) in lost_outputs
        )
        if wasted:
            color, hatch = style.WASTED, "////"
        else:
            color, hatch = (style.BLUE if interval.kind == "map" else style.ORANGE), None
        axis.broken_barh(
            [(interval.start, max(interval.end - interval.start, 0.05))],
            (rows[interval.worker_id] - bar_height / 2, bar_height),
            facecolors=color, edgecolor=style.SURFACE, linewidth=1, hatch=hatch,
        )
        if interval.reason == "rerun" and not wasted:
            axis.text(interval.start + 0.1, rows[interval.worker_id], "re-run", fontsize=7,
                      color=style.SURFACE, va="center", clip_on=True)
    kill_t = result.notes["kill_t"]
    axis.axvline(kill_t, color=style.TEXT_PRIMARY, linestyle="--", linewidth=1.5)
    axis.annotate(f"{result.notes['killed_worker']} killed", (kill_t, len(ALL_WORKERS) - 0.45),
                  xytext=(4, 0), textcoords="offset points", fontsize=9, color=style.TEXT_PRIMARY)
    axis.set_yticks(list(rows.values()), list(rows.keys()))
    axis.set_ylim(-0.6, len(ALL_WORKERS) - 0.2)
    axis.set_xlim(left=0)
    axis.grid(axis="y", visible=False)


def plot_timeline(results_root: Path = RESULTS_ROOT, plots_root: Path = PLOTS_ROOT) -> Path:
    results = load_results(EXPERIMENT, results_root)
    figure, axes = style.new_figure(width=9, height=6.4, rows=2)
    for axis, label in zip(axes[:, 0], [KILL_DURING_MAP, KILL_DURING_REDUCE]):
        first_run = min((r for r in results if r.label == label), key=lambda r: r.run)
        _draw_timeline(axis, first_run, results_root)
        style.title(axis, f"Worker {label}", f"{first_run.job_id}, run {first_run.run}: finished in {first_run.job_seconds:.1f}s")
        style.label_axes(axis, "Seconds since the job was submitted", "")
    axes[0][0].legend(
        handles=[
            Patch(facecolor=style.BLUE, label="Map task"),
            Patch(facecolor=style.ORANGE, label="Reduce task"),
            Patch(facecolor=style.WASTED, hatch="////", edgecolor=style.SURFACE, label="Work lost with the dead worker"),
        ],
        frameon=False, fontsize=9, loc="upper right", bbox_to_anchor=(1, 1.32), ncols=3,
    )
    path = plots_root / "failures_timeline.png"
    style.save(figure, path)
    return path


# ----- plot 4: cost -----


def plot_cost(results_root: Path = RESULTS_ROOT, plots_root: Path = PLOTS_ROOT) -> Path:
    results = load_results(EXPERIMENT, results_root)
    figure, axes = style.new_figure()
    axis = axes[0][0]
    positions = range(len(LABELS))
    for position, label in zip(positions, LABELS):
        runs = [r for r in results if r.label == label]
        times = spread([r.job_seconds for r in runs])
        reruns = sorted(extra_attempts(load_events(r, results_root))["rerun"] for r in runs)
        axis.bar(position, times.median, width=0.55, color=style.BLUE)
        axis.errorbar(position, times.median, yerr=[[times.median - times.low], [times.high - times.median]],
                      color=style.TEXT_PRIMARY, capsize=4, linewidth=1)
        detail = "" if label == NO_FAILURE else f"\n{style.plural(reruns[len(reruns) // 2], 'task')} re-run"
        axis.annotate(f"{times.median:.1f}s{detail}", (position, times.high), textcoords="offset points",
                      xytext=(0, 6), ha="center", fontsize=9, color=style.TEXT_PRIMARY)
    axis.set_xticks(list(positions), [label.capitalize() for label in LABELS])
    axis.set_ylim(0, max(r.job_seconds for r in results) * 1.3)
    axis.grid(axis="x", visible=False)
    style.title(axis, "Cost of losing a worker mid-job",
                f"Median of {style.plural(len(results) // len(LABELS), 'run')}, min–max whiskers; "
                "every run's output matched a single-process count")
    style.label_axes(axis, "", "Job time (s)")
    path = plots_root / "failures_cost.png"
    style.save(figure, path)
    return path


def main() -> None:
    parser = argparse.ArgumentParser(prog="python -m eval.failures", description=__doc__.splitlines()[0])
    commands = parser.add_subparsers(dest="command", required=True)
    run_parser = commands.add_parser("run", help="run the experiment on the cluster")
    run_parser.add_argument("--repeats", type=int, default=3)
    commands.add_parser("plot", help="draw both failure plots from saved results")
    args = parser.parse_args()
    if args.command == "run":
        run(args.repeats)
    else:
        plot_timeline()
        plot_cost()


if __name__ == "__main__":
    main()
