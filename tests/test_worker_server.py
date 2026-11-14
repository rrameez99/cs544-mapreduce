"""Tests for partition serving and worker terminal reports."""

import time
from concurrent import futures

import grpc
import pytest

from protos import mapreduce_pb2 as pb
from protos import mapreduce_pb2_grpc as pb_grpc
from worker.runner import TaskRunResult, TaskRunner, UnfetchableMapOutputError
from worker.server import FETCH_CHUNK_BYTES, AttemptExecutor, WorkerServicer


def start_worker_server(servicer):
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=2))
    pb_grpc.add_WorkerServicer_to_server(servicer, server)
    port = server.add_insecure_port("localhost:0")
    server.start()
    channel = grpc.insecure_channel(f"localhost:{port}")
    grpc.channel_ready_future(channel).result(timeout=2)
    return server, channel, f"localhost:{port}"


def test_fetch_partition_streams_exact_file_in_one_mib_chunks(tmp_path):
    local_root = tmp_path / "local"
    partition_directory = local_root / "job-job-1" / "map-2.3"
    partition_directory.mkdir(parents=True)
    payload = b"x" * (FETCH_CHUNK_BYTES + 17)
    (partition_directory / "part-4.jsonl").write_bytes(payload)
    server, channel, _ = start_worker_server(WorkerServicer(local_root))
    try:
        responses = list(
            pb_grpc.WorkerStub(channel).FetchPartition(
                pb.FetchPartitionRequest(job_id="job-1", map_task_id=2, attempt_id=3, reduce_task_id=4)
            )
        )
    finally:
        channel.close()
        server.stop(0).wait()

    assert b"".join(response.data for response in responses) == payload
    assert [len(response.data) for response in responses] == [FETCH_CHUNK_BYTES, 17]


def test_fetch_partition_returns_not_found_for_missing_attempt(tmp_path):
    server, channel, _ = start_worker_server(WorkerServicer(tmp_path / "local"))
    try:
        with pytest.raises(grpc.RpcError) as failure:
            list(
                pb_grpc.WorkerStub(channel).FetchPartition(
                    pb.FetchPartitionRequest(job_id="job-1", map_task_id=2, attempt_id=3, reduce_task_id=4)
                )
            )
    finally:
        channel.close()
        server.stop(0).wait()

    assert failure.value.code() == grpc.StatusCode.NOT_FOUND


def test_reduce_fetches_a_mapper_partition_over_grpc(tmp_path):
    mapper_local = tmp_path / "mapper-local"
    reducer_local = tmp_path / "reducer-local"
    shared_root = tmp_path / "shared"
    input_directory = shared_root / "input"
    input_directory.mkdir(parents=True)
    (input_directory / "words.txt").write_text("blue blue green\n", encoding="utf-8")
    job_code = b"""
def map(key, line):
    for word in line.split():
        yield word, 1

def reduce(key, values):
    yield key, sum(values)
"""
    map_task = pb.MapTask(
        attempt=pb.TaskAttempt(job_id="job-1", kind=pb.TASK_KIND_MAP, task_id=0, attempt_id=0),
        splits=[pb.InputSplit(path="/shared/input/words.txt", start_offset=0, end_offset=16)],
        num_reduce_tasks=1,
        job_code=job_code,
    )
    TaskRunner(mapper_local, shared_root).run_map(map_task)
    server, channel, fetch_address = start_worker_server(WorkerServicer(mapper_local))
    try:
        reduce_task = pb.ReduceTask(
            attempt=pb.TaskAttempt(job_id="job-1", kind=pb.TASK_KIND_REDUCE, task_id=0, attempt_id=0),
            map_outputs=[pb.MapOutputLocation(map_task_id=0, attempt_id=0, fetch_address=fetch_address)],
            output_path="/shared/output/wc",
            job_code=job_code,
        )
        TaskRunner(reducer_local, shared_root).run_reduce(reduce_task)
    finally:
        channel.close()
        server.stop(0).wait()

    assert (shared_root / "output" / "wc" / "part-r-00000").read_text(encoding="utf-8") == "blue\t2\ngreen\t1\n"


class FakeBoss:
    def __init__(self):
        self.done_reports = []
        self.failed_reports = []

    def ReportTaskDone(self, report, timeout):
        self.done_reports.append((report, timeout))
        return pb.ReportAck(accepted=True)

    def ReportTaskFailed(self, report, timeout):
        self.failed_reports.append((report, timeout))
        return pb.ReportAck(accepted=True)


class SuccessfulRunner:
    def run_map(self, _task):
        return TaskRunResult(metrics=pb.TaskMetrics(records_in=1, records_out=2))


class MissingPartitionRunner:
    def run_reduce(self, _task):
        raise UnfetchableMapOutputError(5, "map output no longer exists")


def wait_for_executor(executor):
    deadline = time.monotonic() + 2
    while executor.is_running and time.monotonic() < deadline:
        time.sleep(0.01)
    assert not executor.is_running


def test_attempt_executor_reports_success_once():
    boss = FakeBoss()
    executor = AttemptExecutor(boss, "worker-1", SuccessfulRunner())
    task = pb.MapTask(
        attempt=pb.TaskAttempt(job_id="job-1", kind=pb.TASK_KIND_MAP, task_id=0, attempt_id=1)
    )

    executor.start(task)
    wait_for_executor(executor)

    assert len(boss.done_reports) == 1
    report, _ = boss.done_reports[0]
    assert report.worker_id == "worker-1"
    assert report.attempt == task.attempt
    assert report.metrics.records_out == 2
    assert boss.failed_reports == []


def test_attempt_executor_reports_unfetchable_map_id():
    boss = FakeBoss()
    executor = AttemptExecutor(boss, "worker-1", MissingPartitionRunner())
    task = pb.ReduceTask(
        attempt=pb.TaskAttempt(job_id="job-1", kind=pb.TASK_KIND_REDUCE, task_id=0, attempt_id=1)
    )

    executor.start(task)
    wait_for_executor(executor)

    assert boss.done_reports == []
    assert len(boss.failed_reports) == 1
    report, _ = boss.failed_reports[0]
    assert report.HasField("unfetchable_map_task_id")
    assert report.unfetchable_map_task_id == 5
