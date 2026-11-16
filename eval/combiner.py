"""Plot 2: how much does a map-side combiner shrink the word-count shuffle?

Three healthy-cluster runs are made without a combiner and three with it. The
input, job, map count, and reduce count are otherwise identical. Each run uses
a fresh cluster and is checked against the job's single-process reference.

    python -m eval.combiner run [--repeats 3]   # on the VM
    python -m eval.combiner plot
"""

import argparse
from pathlib import Path

from eval import style
from eval.analysis import map_output_bytes, spread
from eval.harness import PLOTS_ROOT, RESULTS_ROOT, load_events, load_results, run_job, save_results, start_cluster, stop_cluster

EXPERIMENT = "combiner"
NUM_MAP_TASKS = 16
NUM_REDUCE_TASKS = 4
WITHOUT_COMBINER = "combiner off"
WITH_COMBINER = "combiner on"
LABELS = [WITHOUT_COMBINER, WITH_COMBINER]
COLORS = {WITHOUT_COMBINER: style.ORANGE, WITH_COMBINER: style.AQUA}
BYTES_PER_MB = 1_000_000


def run(repeats: int) -> None:
    """Run each combiner setting repeatedly on a new healthy cluster."""
    results = []
    for label in LABELS:
        for repetition in range(1, repeats + 1):
            start_cluster()
            results.append(
                run_job(
                    EXPERIMENT,
                    label,
                    repetition,
                    NUM_MAP_TASKS,
                    NUM_REDUCE_TASKS,
                    use_combiner=label == WITH_COMBINER,
                )
            )
    stop_cluster()
    print(f"saved {save_results(EXPERIMENT, results)}")


def _draw_bar(axis, position: int, value, color: str, label: str) -> None:
    """Draw one median bar, its min/max whisker, and its numeric value."""
    axis.bar(position, value.median, width=0.55, color=color)
    axis.errorbar(
        position,
        value.median,
        yerr=[[value.median - value.low], [value.high - value.median]],
        color=style.TEXT_PRIMARY,
        capsize=4,
        linewidth=1,
    )
    axis.annotate(
        label,
        (position, value.high),
        textcoords="offset points",
        xytext=(0, 6),
        ha="center",
        fontsize=9,
        color=style.TEXT_PRIMARY,
    )


def plot(results_root: Path = RESULTS_ROOT, plots_root: Path = PLOTS_ROOT) -> Path:
    """Draw intermediate bytes and job time from the six saved event logs."""
    results = load_results(EXPERIMENT, results_root)
    figure, axes = style.new_figure(width=10, height=4.4, columns=2)
    bytes_axis, times_axis = axes[0]

    byte_spreads = {}
    time_spreads = {}
    for label in LABELS:
        runs = [result for result in results if result.label == label]
        byte_spreads[label] = spread(
            [map_output_bytes(load_events(result, results_root)) / BYTES_PER_MB for result in runs]
        )
        time_spreads[label] = spread([result.job_seconds for result in runs])

    for position, label in enumerate(LABELS):
        bytes_written = byte_spreads[label]
        job_times = time_spreads[label]
        _draw_bar(bytes_axis, position, bytes_written, COLORS[label], f"{bytes_written.median:.1f} MB")
        _draw_bar(times_axis, position, job_times, COLORS[label], f"{job_times.median:.1f}s")

    labels = [label.capitalize() for label in LABELS]
    for axis in (bytes_axis, times_axis):
        axis.set_xticks(range(len(LABELS)), labels)
        axis.grid(axis="x", visible=False)
        axis.set_ylim(bottom=0)

    repeats = len(results) // len(LABELS)
    style.title(
        bytes_axis,
        "Intermediate map output",
        f"Median of {style.plural(repeats, 'run')}, min–max whiskers",
    )
    style.label_axes(bytes_axis, "", "Accepted map output (MB)")
    style.title(
        times_axis,
        "Job time",
        f"Word count, {NUM_MAP_TASKS} map / {NUM_REDUCE_TASKS} reduce tasks",
    )
    style.label_axes(times_axis, "", "Job time (s)")

    path = plots_root / "combiner.png"
    style.save(figure, path)
    return path


def main() -> None:
    parser = argparse.ArgumentParser(prog="python -m eval.combiner", description=__doc__.splitlines()[0])
    commands = parser.add_subparsers(dest="command", required=True)
    run_parser = commands.add_parser("run", help="run the combiner comparison on the cluster")
    run_parser.add_argument("--repeats", type=int, default=3)
    commands.add_parser("plot", help="draw eval/plots/combiner.png from saved results")
    args = parser.parse_args()
    if args.command == "run":
        run(args.repeats)
    else:
        plot()


if __name__ == "__main__":
    main()
