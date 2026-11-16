"""Tests for the deterministic offline evaluation-data generator."""

import pytest

from scripts.make_data import DATASET_FILE_NAME, DatasetError, make_dataset, ordered_corpus


def test_make_dataset_sorts_sources_repeats_whole_utf8_corpus_and_keeps_lines(tmp_path):
    later = tmp_path / "z-last.txt"
    first = tmp_path / "a-first.txt"
    later.write_text("zebra\n", encoding="utf-8")
    first.write_text("ant 🐜", encoding="utf-8")  # Deliberately no final newline.

    output_directory = tmp_path / "books"
    expected_corpus = first.read_bytes() + b"\n" + later.read_bytes()
    sources, dataset_path = make_dataset([later, first], output_directory, len(expected_corpus) + 1)

    assert sources == [first.resolve(), later.resolve()]
    assert dataset_path == output_directory / DATASET_FILE_NAME
    assert dataset_path.read_bytes() == expected_corpus * 2
    assert dataset_path.read_text(encoding="utf-8") == "ant 🐜\nzebra\nant 🐜\nzebra\n"
    assert len(expected_corpus) + 1 <= dataset_path.stat().st_size < len(expected_corpus) * 2 + 1


def test_make_dataset_rejects_invalid_sources_and_existing_destination(tmp_path):
    text = tmp_path / "book.txt"
    text.write_text("a\n", encoding="utf-8")
    invalid_utf8 = tmp_path / "bad.txt"
    invalid_utf8.write_bytes(b"\xff")

    with pytest.raises(DatasetError, match="not UTF-8"):
        ordered_corpus([invalid_utf8])
    with pytest.raises(DatasetError, match="at least one"):
        ordered_corpus([])

    output_directory = tmp_path / "books"
    output_directory.mkdir()
    with pytest.raises(DatasetError, match="already exists"):
        make_dataset([text], output_directory, 10)
