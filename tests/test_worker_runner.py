"""Unit tests for worker-side map, shuffle, and reduce execution."""

import json
import os
import subprocess
import sys
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import pytest

from boss.jobs import compute_splits
from protos import mapreduce_pb2 as pb
from worker.runner import (
    TaskExecutionError,
    TaskRunner,
    UnfetchableMapOutputError,
    iter_split_records,
    partition_for_key,
)


WORD_COUNT_CODE = b"""
def map(key, line):
    for word in line.split():
        yield word.lower(), 1

def combine(key, values):
    yield key, sum(values)

def reduce(key, values):
    yield key, sum(values)
"""


@pytest.fixture
def roots(tmp_path):
    local_root = tmp_path / "local"
    shared_root = tmp_path / "shared"
    local_root.mkdir()
    shared_root.mkdir()
    return local_root, shared_root


def map_task(
    *,
    task_id=0,
    attempt_id=0,
    splits=(),
    reducers=2,
    use_combiner=False,
    job_code=WORD_COUNT_CODE,
):
    return pb.MapTask(
        attempt=pb.TaskAttempt(
            job_id="job-1",
            kind=pb.TASK_KIND_MAP,
            task_id=task_id,
            attempt_id=attempt_id,
        ),
        splits=splits,
        num_reduce_tasks=reducers,
        job_code=job_code,
        use_combiner=use_combiner,
    )


def reduce_task(*, attempt_id=0, locations=(), job_code=WORD_COUNT_CODE):
    return pb.ReduceTask(
        attempt=pb.TaskAttempt(
            job_id="job-1",
            kind=pb.TASK_KIND_REDUCE,
            task_id=0,
            attempt_id=attempt_id,
        ),
        map_outputs=locations,
        output_path="/shared/output/wc",
        job_code=job_code,
    )


def read_map_records(local_root: Path, task: pb.MapTask):
    records = []
    attempt_directory = (
        local_root / task.attempt.job_id / f"map-{task.attempt.task_id}.{task.attempt.attempt_id}"
    )
    for path in sorted(attempt_directory.glob("part-*.jsonl")):
        records.extend(json.loads(line) for line in path.read_text(encoding="utf-8").splitlines())
    return records


def test_partitioner_is_stable_across_python_hash_seeds():
    script = "from worker.runner import partition_for_key; print(partition_for_key('consistent-key', 17))"
    outputs = []
    for seed in ("1", "2", "random"):
        environment = os.environ | {"PYTHONHASHSEED": seed}
        result = subprocess.run(
            [sys.executable, "-c", script],
            check=True,
            capture_output=True,
            text=True,
            env=environment,
        )
        outputs.append(result.stdout.strip())
    assert outputs == [outputs[0]] * 3
    assert partition_for_key("consistent-key", 17) == int(outputs[0])


def test_map_reader_uses_line_start_offsets_and_publishes_each_partition(roots):
    local_root, shared_root = roots
    input_directory = shared_root / "input"
    input_directory.mkdir()
    input_path = input_directory / "records.txt"
    input_path.write_bytes(b"alpha\nbravo\ncharlie\n")
    task = map_task(
        splits=[pb.InputSplit(path="/shared/input/records.txt", start_offset=2, end_offset=13)]
    )

    result = TaskRunner(local_root, shared_root).run_map(task)

    assert result.metrics.records_in == 2
    assert sorted(read_map_records(local_root, task)) == [["bravo", 1], ["charlie", 1]]
    attempt_directory = local_root / "job-1" / "map-0.0"
    assert len(list(attempt_directory.glob("part-*.jsonl"))) == 2
    assert not list(attempt_directory.glob("*.tmp"))
    assert result.metrics.bytes_written == sum(
        path.stat().st_size for path in attempt_directory.glob("part-*.jsonl")
    )


@pytest.mark.parametrize("num_map_tasks", [1, 2, 3, 7])
def test_all_computed_splits_read_each_line_once(num_map_tasks, roots):
    """Byte-range splits retain whole lines without gaps or overlap."""
    _, shared_root = roots
    input_directory = shared_root / "input"
    input_directory.mkdir()
    input_path = input_directory / "records.txt"
    raw_lines = [
        b"a\n",
        b"bravo-long\n",
        b"c\n",
        b"delta-delta\n",
        b"echo\r\n",
        b"foxtrot-is-longer\n",
        b"g\n",
        b"hotel\n",
        b"india-india-india\n",
        b"j\n",
        b"kilo\n",
        b"lima-is-a-different-length\n",
        b"m\n",
        b"november\n",
        b"oscar-oscar\n",
        b"p\n",
        b"quebec\n",
        b"romeo-is-longer-still\n",
        b"s\n",
        b"final-line-without-a-newline",
    ]
    input_path.write_bytes(b"".join(raw_lines))
    expected_records = []
    offset = 0
    for raw_line in raw_lines:
        line = raw_line.removesuffix(b"\n").removesuffix(b"\r").decode("utf-8")
        expected_records.append((offset, line))
        offset += len(raw_line)

    splits_by_task = compute_splits(
        [("/shared/input/records.txt", input_path.stat().st_size)], num_map_tasks
    )
    records = [
        record
        for task_splits in splits_by_task
        for split in task_splits
        for record in iter_split_records(split, shared_root)
    ]

    assert records == expected_records
    assert len({record_offset for record_offset, _ in records}) == len(expected_records)


def test_empty_map_task_writes_empty_partition_files(roots):
    local_root, shared_root = roots
    task = map_task(splits=[], reducers=3)

    result = TaskRunner(local_root, shared_root).run_map(task)

    attempt_directory = local_root / "job-1" / "map-0.0"
    assert result.metrics.records_in == 0
    assert result.metrics.records_out == 0
    assert result.metrics.bytes_written == 0
    assert [path.read_bytes() for path in sorted(attempt_directory.glob("part-*.jsonl"))] == [b"", b"", b""]


def test_combiner_reduces_intermediate_records_without_changing_counts(roots):
    local_root, shared_root = roots
    input_directory = shared_root / "input"
    input_directory.mkdir()
    (input_directory / "words.txt").write_text("the the the cat\n", encoding="utf-8")
    split = pb.InputSplit(path="/shared/input/words.txt", start_offset=0, end_offset=16)
    without_combiner = map_task(task_id=0, attempt_id=0, splits=[split], reducers=1)
    with_combiner = map_task(task_id=1, attempt_id=0, splits=[split], reducers=1, use_combiner=True)
    runner = TaskRunner(local_root, shared_root)

    normal_result = runner.run_map(without_combiner)
    combined_result = runner.run_map(with_combiner)

    assert sorted(read_map_records(local_root, without_combiner)) == [
        ["cat", 1],
        ["the", 1],
        ["the", 1],
        ["the", 1],
    ]
    assert sorted(read_map_records(local_root, with_combiner)) == [["cat", 1], ["the", 3]]
    assert combined_result.metrics.records_out < normal_result.metrics.records_out
    assert combined_result.metrics.bytes_written < normal_result.metrics.bytes_written


def test_requested_combiner_must_exist(roots):
    local_root, shared_root = roots
    no_combiner_code = b"""
def map(key, line):
    yield line, 1

def reduce(key, values):
    yield key, sum(values)
"""
    task = map_task(splits=[], use_combiner=True, job_code=no_combiner_code)

    with pytest.raises(TaskExecutionError, match="combiner"):
        TaskRunner(local_root, shared_root).run_map(task)


def test_reduce_reassembles_chunked_jsonl_and_groups_sorted_keys(roots):
    local_root, shared_root = roots
    (shared_root / "output" / "wc").mkdir(parents=True)
    location = pb.MapOutputLocation(map_task_id=4, attempt_id=2, fetch_address="mapper:5441")
    task = reduce_task(locations=[location])

    def fetch_partition(_location, _task):
        return [b'["b",1]\n["a",2', b']\n["a",3]\n']

    result = TaskRunner(local_root, shared_root, fetch_partition=fetch_partition).run_reduce(task)

    output = (shared_root / "output" / "wc" / "part-r-00000").read_text(encoding="utf-8")
    assert output == "a\t5\nb\t1\n"
    assert result.metrics.records_in == 3
    assert result.metrics.records_out == 2
    assert result.metrics.bytes_fetched == len(b'["b",1]\n["a",2') + len(b']\n["a",3]\n')
    assert result.metrics.bytes_written == len(output.encode("utf-8"))


def test_reduce_fails_if_boss_did_not_create_output_directory(roots):
    local_root, shared_root = roots

    with pytest.raises(TaskExecutionError, match="cannot open output"):
        TaskRunner(local_root, shared_root).run_reduce(reduce_task())


def test_reduce_surfaces_missing_map_output_with_map_task_id(roots):
    local_root, shared_root = roots
    location = pb.MapOutputLocation(map_task_id=7, attempt_id=0, fetch_address="missing:5441")

    def missing_partition(_location, _task):
        raise UnfetchableMapOutputError(7, "mapper partition is gone")

    with pytest.raises(UnfetchableMapOutputError, match="gone") as failure:
        TaskRunner(local_root, shared_root, fetch_partition=missing_partition).run_reduce(
            reduce_task(locations=[location])
        )
    assert failure.value.map_task_id == 7


def test_competing_reduce_attempts_leave_one_complete_output_and_no_temp_files(roots):
    local_root, shared_root = roots
    (shared_root / "output" / "wc").mkdir(parents=True)
    location = pb.MapOutputLocation(map_task_id=0, attempt_id=0, fetch_address="mapper:5441")

    def fetch_partition(_location, task):
        value = "first" if task.attempt.attempt_id == 0 else "second"
        return [json.dumps(["key", value]).encode("utf-8") + b"\n"]

    reduce_code = b"""
def map(key, line):
    yield key, line

def reduce(key, values):
    for value in values:
        yield key, value
"""
    tasks = [
        reduce_task(attempt_id=attempt_id, locations=[location], job_code=reduce_code)
        for attempt_id in (0, 1)
    ]

    with ThreadPoolExecutor(max_workers=2) as executor:
        list(
            executor.map(
                lambda task: TaskRunner(
                    local_root / f"worker-{task.attempt.attempt_id}", shared_root, fetch_partition
                ).run_reduce(task),
                tasks,
            )
        )

    output_directory = shared_root / "output" / "wc"
    assert (output_directory / "part-r-00000").read_text(encoding="utf-8") in {"key\tfirst\n", "key\tsecond\n"}
    assert not list(output_directory.glob("*.tmp"))
