"""Tests for extracting and plotting the design's combiner evaluation data."""

from pathlib import Path

import pytest

from eval.combiner import (
    CONFIGURATION_OFF,
    CONFIGURATION_ON,
    CombinerResult,
    EvaluationError,
    accepted_map_metrics,
    append_result,
    outputs_match,
    plot_combiner_results,
    read_combiner_results,
    summarize,
)


def successful_events():
    return [
        {"event": "job_submitted", "job_id": "job-1", "t": 0.0},
        {
            "event": "attempt_finished",
            "kind": "map",
            "task_id": 0,
            "outcome": "accepted",
            "bytes_written": 20,
        },
        {
            "event": "attempt_finished",
            "kind": "map",
            "task_id": 0,
            "outcome": "duplicate",
            "bytes_written": 200,
        },
        {
            "event": "attempt_finished",
            "kind": "map",
            "task_id": 1,
            "outcome": "accepted",
            "bytes_written": 30,
        },
        {"event": "job_finished", "state": "SUCCEEDED", "t": 4.25, "failure_reason": ""},
    ]


def test_accepted_map_metrics_counts_only_accepted_map_attempts():
    assert accepted_map_metrics(successful_events(), expected_map_tasks=2) == (4.25, 50)


def test_accepted_map_metrics_rejects_a_malformed_accepted_task_id():
    events = successful_events()
    events[1]["task_id"] = "not-a-task-id"

    with pytest.raises(EvaluationError, match="task_id"):
        accepted_map_metrics(events, expected_map_tasks=2)


@pytest.mark.parametrize(
    "events, message",
    [
        ([], "empty"),
        ([{"event": "job_submitted"}], "job_finished"),
        (
            [
                {"event": "job_submitted"},
                {"event": "job_finished", "state": "FAILED", "failure_reason": "map error"},
            ],
            "did not succeed",
        ),
        (
            [
                {"event": "job_submitted"},
                {
                    "event": "attempt_finished",
                    "kind": "map",
                    "task_id": 0,
                    "outcome": "accepted",
                    "bytes_written": 1,
                },
                {"event": "job_finished", "state": "SUCCEEDED", "t": 1},
            ],
            "incomplete",
        ),
    ],
)
def test_accepted_map_metrics_rejects_missing_failed_and_incomplete_logs(events, message):
    with pytest.raises(EvaluationError, match=message):
        accepted_map_metrics(events, expected_map_tasks=2)


def make_result(configuration, run_number, job_seconds, accepted_map_bytes):
    return CombinerResult(
        run_id="combiner-test",
        configuration=configuration,
        run_number=run_number,
        num_map_tasks=8,
        num_reduce_tasks=4,
        input_path="/input/books",
        output_path=f"/output/{configuration}-{run_number}",
        job_id=f"job-{configuration}-{run_number}",
        job_seconds=job_seconds,
        accepted_map_bytes=accepted_map_bytes,
    )


def complete_results():
    return [
        make_result(CONFIGURATION_OFF, 1, 12.0, 300),
        make_result(CONFIGURATION_OFF, 2, 10.0, 200),
        make_result(CONFIGURATION_OFF, 3, 11.0, 250),
        make_result(CONFIGURATION_ON, 1, 8.0, 80),
        make_result(CONFIGURATION_ON, 2, 7.0, 70),
        make_result(CONFIGURATION_ON, 3, 9.0, 90),
    ]


def test_results_csv_aggregation_and_plot_creation(tmp_path):
    results_path = tmp_path / "results.csv"
    results = complete_results()
    for result in results:
        append_result(results_path, result)

    loaded = read_combiner_results(results_path, "combiner-test")
    assert summarize(loaded, CONFIGURATION_OFF, "accepted_map_bytes") == (250.0, 200.0, 300.0)
    assert summarize(loaded, CONFIGURATION_ON, "job_seconds") == (8.0, 7.0, 9.0)

    plot_path = tmp_path / "plots" / "combiner.png"
    plot_combiner_results(loaded, plot_path)
    assert plot_path.read_bytes().startswith(b"\x89PNG\r\n\x1a\n")


def test_read_results_rejects_an_incomplete_six_run_comparison(tmp_path):
    results_path = tmp_path / "results.csv"
    append_result(results_path, complete_results()[0])

    with pytest.raises(EvaluationError, match="expected 6"):
        read_combiner_results(results_path, "combiner-test")


def test_outputs_match_compares_all_reducer_parts(tmp_path):
    reference = tmp_path / "reference"
    same = tmp_path / "same"
    different = tmp_path / "different"
    for directory in (reference, same, different):
        directory.mkdir()
        (directory / "part-r-00000").write_bytes(b"ant\t2\n")
        (directory / "part-r-00001").write_bytes(b"zebra\t1\n")
    (different / "part-r-00001").write_bytes(b"zebra\t2\n")

    assert outputs_match(reference, same, expected_reduce_tasks=2)
    assert not outputs_match(reference, different, expected_reduce_tasks=2)
