"""Plot 5: do backup tasks rescue a job from a straggler? (like Figure 3 of the paper)

Three configurations, same job:
  - normal:              4 healthy workers
  - straggler:           worker-4 gets 0.2 CPUs (docker-compose.straggler.yml)
  - straggler + backups: the same slow worker, with --backup-tasks

The left panel shows tasks completed over time for one typical run of each:
with a straggler, the last few tasks drag on; with backups, idle workers
re-run them and the tail disappears. The right panel compares job times.

    python -m eval.backup run [--repeats 3]   # on the VM
    python -m eval.backup plot
"""

import argparse
from pathlib import Path

from eval import style
from eval.analysis import completion_curve, extra_attempts, spread
from eval.harness import PLOTS_ROOT, RESULTS_ROOT, load_events, load_results, run_job, save_results, start_cluster, stop_cluster

EXPERIMENT = "backup"
NUM_MAP_TASKS = 16
NUM_REDUCE_TASKS = 4
NORMAL = "normal"
STRAGGLER = "straggler"
STRAGGLER_WITH_BACKUPS = "straggler + backups"
LABELS = [NORMAL, STRAGGLER, STRAGGLER_WITH_BACKUPS]
COLORS = {NORMAL: style.BLUE, STRAGGLER: style.ORANGE, STRAGGLER_WITH_BACKUPS: style.AQUA}


def run(repeats: int) -> None:
    results = []
    for label in LABELS:
        for repetition in range(1, repeats + 1):
            start_cluster(straggler=label != NORMAL)
            results.append(
                run_job(EXPERIMENT, label, repetition, NUM_MAP_TASKS, NUM_REDUCE_TASKS,
                        use_backup_tasks=label == STRAGGLER_WITH_BACKUPS)
            )
    stop_cluster()
    print(f"saved {save_results(EXPERIMENT, results)}")


def plot(results_root: Path = RESULTS_ROOT, plots_root: Path = PLOTS_ROOT) -> Path:
    results = load_results(EXPERIMENT, results_root)
    figure, axes = style.new_figure(width=10, height=4.4, columns=2)
    curves_axis, times_axis = axes[0]

    for position, label in enumerate(LABELS):
        runs = sorted((r for r in results if r.label == label), key=lambda r: r.job_seconds)
        typical = runs[len(runs) // 2]  # the median run, so one lucky or unlucky run doesn't skew the picture
        events = load_events(typical, results_root)
        times, completed = zip(*completion_curve(events))
        curves_axis.step(times, completed, where="post", color=COLORS[label], linewidth=2,
                         label=f"{label.capitalize()} ({typical.job_seconds:.1f}s)")

        job_times = spread([r.job_seconds for r in runs])
        times_axis.bar(position, job_times.median, width=0.55, color=COLORS[label])
        times_axis.errorbar(position, job_times.median,
                            yerr=[[job_times.median - job_times.low], [job_times.high - job_times.median]],
                            color=style.TEXT_PRIMARY, capsize=4, linewidth=1)
        backups = extra_attempts(events)["backup"]
        detail = f"\n{style.plural(backups, 'backup attempt')}" if label == STRAGGLER_WITH_BACKUPS else ""
        times_axis.annotate(f"{job_times.median:.1f}s{detail}", (position, job_times.high), textcoords="offset points",
                            xytext=(0, 6), ha="center", fontsize=9, color=style.TEXT_PRIMARY)

    total_tasks = NUM_MAP_TASKS + NUM_REDUCE_TASKS
    curves_axis.axhline(total_tasks, color=style.GRID, linewidth=1)
    curves_axis.set_ylim(0, total_tasks * 1.12)
    curves_axis.set_yticks(range(0, total_tasks + 1, 5))
    curves_axis.set_xlim(left=0)
    style.title(curves_axis, "Tasks completed over time", f"Median run of each; {total_tasks} tasks per job")
    style.label_axes(curves_axis, "Seconds since the job was submitted", "Tasks completed")
    curves_axis.legend(frameon=False, fontsize=9, loc="lower right", labelcolor=style.TEXT_PRIMARY)

    times_axis.set_xticks(range(len(LABELS)), [label.capitalize() for label in LABELS])
    times_axis.set_ylim(0, max(r.job_seconds for r in results) * 1.3)
    times_axis.grid(axis="x", visible=False)
    style.title(times_axis, "Job time", f"Median of {style.plural(len(results) // len(LABELS), 'run')}, min–max whiskers")
    style.label_axes(times_axis, "", "Job time (s)")

    path = plots_root / "backup_tasks.png"
    style.save(figure, path)
    return path


def main() -> None:
    parser = argparse.ArgumentParser(prog="python -m eval.backup", description=__doc__.splitlines()[0])
    commands = parser.add_subparsers(dest="command", required=True)
    run_parser = commands.add_parser("run", help="run the experiment on the cluster")
    run_parser.add_argument("--repeats", type=int, default=3)
    commands.add_parser("plot", help="draw eval/plots/backup_tasks.png from saved results")
    args = parser.parse_args()
    if args.command == "run":
        run(args.repeats)
    else:
        plot()


if __name__ == "__main__":
    main()
