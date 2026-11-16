"""One look for every evaluation plot: colors, fonts, recessive axes and grid."""

import matplotlib

matplotlib.use("Agg")  # render to files; the VM has no display

import matplotlib.pyplot as plt  # noqa: E402  (must come after choosing the backend)

SURFACE = "#fcfcfb"
TEXT_PRIMARY = "#0b0b0b"
TEXT_SECONDARY = "#52514e"
GRID = "#e4e3df"

# Categorical colors, always in this order and always for the same meaning.
BLUE = "#2a78d6"  # map work / first series
ORANGE = "#eb6834"  # reduce work / second series
AQUA = "#1baf7a"  # third series
WASTED = "#b9b8b2"  # attempts whose work was thrown away (lost, duplicate, failed)


def new_figure(width: float = 7.0, height: float = 4.2, columns: int = 1, rows: int = 1):
    figure, axes = plt.subplots(rows, columns, figsize=(width, height), facecolor=SURFACE, squeeze=False)
    for axis in axes.flat:
        style_axis(axis)
    return figure, axes


def style_axis(axis) -> None:
    axis.set_facecolor(SURFACE)
    for side in ("top", "right"):
        axis.spines[side].set_visible(False)
    for side in ("left", "bottom"):
        axis.spines[side].set_color(TEXT_SECONDARY)
        axis.spines[side].set_linewidth(0.8)
    axis.tick_params(colors=TEXT_SECONDARY, labelsize=9)
    axis.grid(color=GRID, linewidth=0.8)
    axis.set_axisbelow(True)


def title(axis, text: str, subtitle: str = "") -> None:
    axis.set_title(text, loc="left", fontsize=11, fontweight="bold", color=TEXT_PRIMARY, pad=18 if subtitle else 8)
    if subtitle:
        axis.text(0, 1.02, subtitle, transform=axis.transAxes, fontsize=9, color=TEXT_SECONDARY)


def label_axes(axis, x: str, y: str) -> None:
    axis.set_xlabel(x, fontsize=9, color=TEXT_SECONDARY)
    axis.set_ylabel(y, fontsize=9, color=TEXT_SECONDARY)


def plural(count: int, noun: str) -> str:
    return f"{count} {noun}" + ("" if count == 1 else "s")


def save(figure, path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    figure.tight_layout()
    figure.savefig(path, dpi=160, facecolor=SURFACE)
    plt.close(figure)
    print(f"wrote {path}")
