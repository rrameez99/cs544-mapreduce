"""Build a deterministic line-oriented evaluation dataset from local Gutenberg texts.

The script stays offline: source files must already be downloaded.  It orders
those UTF-8 files by path, joins them into one newline-terminated corpus, and
repeats the complete corpus until the requested size has been reached.  It
never truncates a source file or a UTF-8 character.
"""

import argparse
import sys
from pathlib import Path


DEFAULT_TARGET_BYTES = 200 * 1024 * 1024
DEFAULT_OUTPUT_DIRECTORY = Path("shared/input/books")
DATASET_FILE_NAME = "books.txt"


class DatasetError(ValueError):
    """A source file or requested destination cannot make a valid dataset."""


def positive_int(value: str) -> int:
    """Parse a strictly positive byte target for argparse."""
    try:
        number = int(value)
    except ValueError as error:
        raise argparse.ArgumentTypeError(f"expected an integer, got {value!r}") from error
    if number < 1:
        raise argparse.ArgumentTypeError(f"must be at least 1, got {number}")
    return number


def _read_utf8_source(path: Path) -> bytes:
    """Read one nonempty UTF-8 file and make its final line explicit."""
    if not path.is_file():
        raise DatasetError(f"source is not a file: {path}")
    try:
        content = path.read_bytes()
    except OSError as error:
        raise DatasetError(f"could not read source {path}: {error}") from error
    try:
        content.decode("utf-8")
    except UnicodeDecodeError as error:
        raise DatasetError(f"source is not UTF-8: {path}: {error}") from error
    if not content:
        raise DatasetError(f"source is empty: {path}")
    return content if content.endswith(b"\n") else content + b"\n"


def ordered_corpus(source_paths: list[Path]) -> tuple[list[Path], bytes]:
    """Return canonical source order and their line-oriented UTF-8 corpus."""
    if not source_paths:
        raise DatasetError("provide at least one Gutenberg UTF-8 text file")
    sources = sorted((path.resolve() for path in source_paths), key=lambda path: path.as_posix())
    return sources, b"".join(_read_utf8_source(path) for path in sources)


def make_dataset(source_paths: list[Path], output_directory: Path, target_bytes: int) -> tuple[list[Path], Path]:
    """Write complete corpus repetitions and return the source order and data file.

    The requested size is a lower bound.  Keeping every repetition whole makes
    the result line-oriented and valid UTF-8; its size is less than one corpus
    larger than ``target_bytes``.
    """
    if target_bytes < 1:
        raise DatasetError(f"target bytes must be at least 1, got {target_bytes}")
    sources, corpus = ordered_corpus(source_paths)
    if output_directory.exists() or output_directory.is_symlink():
        raise DatasetError(f"output directory already exists: {output_directory}")

    try:
        output_directory.mkdir(parents=True)
        dataset_path = output_directory / DATASET_FILE_NAME
        bytes_written = 0
        with dataset_path.open("xb") as dataset_file:
            while bytes_written < target_bytes:
                dataset_file.write(corpus)
                bytes_written += len(corpus)
    except OSError as error:
        raise DatasetError(f"could not write dataset in {output_directory}: {error}") from error

    # Keep this assertion next to the write loop: the documented size bound is
    # a property of whole-corpus repetitions, not an assumption about sources.
    assert target_bytes <= bytes_written < target_bytes + len(corpus)
    return sources, dataset_path


def build_parser() -> argparse.ArgumentParser:
    """Construct the offline Gutenberg-dataset command line."""
    parser = argparse.ArgumentParser(description="Create a repeated UTF-8 Gutenberg dataset")
    parser.add_argument("sources", nargs="+", type=Path, help="local UTF-8 Gutenberg text files")
    parser.add_argument(
        "--output",
        type=Path,
        default=DEFAULT_OUTPUT_DIRECTORY,
        help="directory for books.txt (default: shared/input/books)",
    )
    parser.add_argument(
        "--target-bytes",
        type=positive_int,
        default=DEFAULT_TARGET_BYTES,
        help=f"minimum dataset size in bytes (default: {DEFAULT_TARGET_BYTES})",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    """Create the requested dataset and print its reproducibility details."""
    args = build_parser().parse_args(argv)
    try:
        sources, dataset_path = make_dataset(args.sources, args.output, args.target_bytes)
    except DatasetError as error:
        print(f"error: {error}", file=sys.stderr)
        return 1

    print("Sources:")
    for source in sources:
        print(f"- {source}")
    print(f"Output: {dataset_path}")
    print(f"Final byte count: {dataset_path.stat().st_size}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
