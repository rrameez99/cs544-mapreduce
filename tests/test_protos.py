"""Checks that the generated protobuf code builds and behaves as the design expects."""

from protos import mapreduce_pb2 as pb
from protos import mapreduce_pb2_grpc as pb_grpc


def test_heartbeat_response_round_trip_keeps_map_task():
    attempt = pb.TaskAttempt(job_id="job-1", kind=pb.TASK_KIND_MAP, task_id=3, attempt_id=1)
    split = pb.InputSplit(path="/shared/input/books/a.txt", start_offset=0, end_offset=1024)
    response = pb.HeartbeatResponse(
        map_task=pb.MapTask(attempt=attempt, splits=[split], num_reduce_tasks=4, job_code=b"def map(k, v): ...")
    )

    parsed = pb.HeartbeatResponse.FromString(response.SerializeToString())

    assert parsed.WhichOneof("assignment") == "map_task"
    assert parsed.map_task.attempt == attempt
    assert list(parsed.map_task.splits) == [split]


def test_empty_heartbeat_response_means_no_work():
    assert pb.HeartbeatResponse().WhichOneof("assignment") is None


def test_unfetchable_map_task_id_distinguishes_unset_from_zero():
    report = pb.TaskFailedReport(worker_id="worker-1", error="user code raised")
    assert not report.HasField("unfetchable_map_task_id")

    report.unfetchable_map_task_id = 0
    assert report.HasField("unfetchable_map_task_id")


def test_services_are_generated():
    assert hasattr(pb_grpc, "BossStub")
    assert hasattr(pb_grpc, "add_BossServicer_to_server")
    assert hasattr(pb_grpc, "WorkerStub")
    assert hasattr(pb_grpc, "add_WorkerServicer_to_server")
