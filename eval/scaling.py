"""Plot 1: does the job get faster as we add workers?

Each worker container is limited to 1 CPU, so with the same job, going from 1
to 4 workers should cut the job time by up to 4x if tasks really spread out.

    python -m eval.scaling run [--repeats 3]   # on the VM, takes a few minutes
    python -m eval.scaling plot                # writes eval/plots/scaling.png
"""

import argparse
from pathlib import Path

from eval import style
from eval.analysis import spread
from eval.harness import ALL_WORKERS, PLOTS_ROOT, RESULTS_ROOT, load_results, run_job, save_results, start_cluster, stop_cluster

EXPERIMENT = "scaling"
NUM_MAP_TASKS = 16
NUM_REDUCE_TASKS = 4


def label_for(num_workers: int) -> str:
    return f"{num_workers} worker" + ("" if num_workers == 1 else "s")


def run(repeats: int) -> None:
    results = []
    for num_workers in range(1, len(ALL_WORKERS) + 1):
        for repetition in range(1, repeats + 1):
            start_cluster(num_workers)  # a fresh cluster per run, so runs don't affect each other
            results.append(run_job(EXPERIMENT, label_for(num_workers), repetition, NUM_MAP_TASKS, NUM_REDUCE_TASKS))
    stop_cluster()
    print(f"saved {save_results(EXPERIMENT, results)}")


def plot(results_root: Path = RESULTS_ROOT, plots_root: Path = PLOTS_ROOT) -> Path:
    results = load_results(EXPERIMENT, results_root)
    worker_counts = sorted({int(result.label.split()[0]) for result in results})
    spreads = [spread([r.job_seconds for r in results if r.label == label_for(n)]) for n in worker_counts]
    medians = [s.median for s in spreads]

    figure, axes = style.new_figure()
    axis = axes[0][0]
    one_worker = medians[worker_counts.index(1)] if 1 in worker_counts else medians[0]
    axis.plot(worker_counts, [one_worker / n for n in worker_counts], linestyle="--", linewidth=1.5,
              color=style.TEXT_SECONDARY, label="Ideal (1-worker time ÷ workers)")
    axis.errorbar(worker_counts, medians,
                  yerr=[[m - s.low for m, s in zip(medians, spreads)], [s.high - m for m, s in zip(medians, spreads)]],
                  color=style.BLUE, linewidth=2, marker="o", markersize=8, capsize=4, label="Measured (median, min–max)")
    for n, m in zip(worker_counts, medians):
        axis.annotate(f"{m:.1f}s", (n, m), textcoords="offset points", xytext=(10, -14), fontsize=9, color=style.TEXT_PRIMARY)

    axis.set_xticks(worker_counts)
    axis.set_ylim(bottom=0)
    speedup = medians[0] / medians[-1]
    style.title(axis, "Job time vs. number of workers",
                f"Word count, {NUM_MAP_TASKS} map / {NUM_REDUCE_TASKS} reduce tasks, 1 CPU per worker · "
                f"{speedup:.1f}× faster with {worker_counts[-1]} workers")
    style.label_axes(axis, "Workers", "Job time (s)")
    axis.legend(frameon=False, fontsize=9, labelcolor=style.TEXT_PRIMARY)
    path = plots_root / "scaling.png"
    style.save(figure, path)
    return path


def main() -> None:
    parser = argparse.ArgumentParser(prog="python -m eval.scaling", description=__doc__.splitlines()[0])
    commands = parser.add_subparsers(dest="command", required=True)
    run_parser = commands.add_parser("run", help="run the experiment on the cluster")
    run_parser.add_argument("--repeats", type=int, default=3)
    commands.add_parser("plot", help="draw eval/plots/scaling.png from saved results")
    args = parser.parse_args()
    if args.command == "run":
        run(args.repeats)
    else:
        plot()


if __name__ == "__main__":
    main()
