"""Tests for the host-side MapReduce client commands."""

import io
from concurrent import futures
from pathlib import Path

import grpc
import pytest

from client.cli import (
    ClientError,
    build_job_spec,
    cat_output,
    container_shared_path,
    host_shared_path,
    main,
    wait_for_job,
)
from protos import mapreduce_pb2 as pb
from protos import mapreduce_pb2_grpc as pb_grpc


class ClientBoss(pb_grpc.BossServicer):
    def __init__(self):
        self.submissions = []
        self.statuses = {}
        self.status_sequences = {}

    def SubmitJob(self, request, _context):
        self.submissions.append(request)
        return pb.SubmitJobResponse(job_id="job-42")

    def GetJobStatus(self, request, context):
        sequence = self.status_sequences.get(request.job_id)
        if sequence:
            return sequence.pop(0)
        status = self.statuses.get(request.job_id)
        if status is None:
            context.abort(grpc.StatusCode.NOT_FOUND, f"no job with ID {request.job_id!r}")
        return status


@pytest.fixture
def shared_root(tmp_path):
    root = tmp_path / "shared"
    root.mkdir()
    return root


@pytest.fixture
def client_boss():
    servicer = ClientBoss()
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=2))
    pb_grpc.add_BossServicer_to_server(servicer, server)
    port = server.add_insecure_port("localhost:0")
    server.start()
    channel = grpc.insecure_channel(f"localhost:{port}")
    grpc.channel_ready_future(channel).result(timeout=2)
    channel.close()
    try:
        yield servicer, f"localhost:{port}"
    finally:
        server.stop(grace=None)


def run_client(argv, shared_root, boss_address="localhost:5440", binary_stdout=None):
    stdout = io.StringIO()
    stderr = io.StringIO()
    exit_code = main(
        argv,
        shared_root=shared_root,
        boss_address=boss_address,
        stdout=stdout,
        stderr=stderr,
        binary_stdout=binary_stdout,
    )
    return exit_code, stdout.getvalue(), stderr.getvalue()


@pytest.mark.parametrize(
    ("client_path", "container_path"),
    [
        ("/input/books", "/shared/input/books"),
        ("/output/wc", "/shared/output/wc"),
    ],
)
def test_client_paths_translate_to_the_container_shared_namespace(client_path, container_path, shared_root):
    assert container_shared_path(client_path) == container_path
    assert host_shared_path(client_path, shared_root) == shared_root / Path(*client_path.split("/")[1:])


@pytest.mark.parametrize("bad_path", ["input/books", "/", "/shared/input/books", "/input/../outside", r"/input\\books"])
def test_client_paths_reject_non_shared_namespace_or_traversal(bad_path, shared_root):
    with pytest.raises(ClientError):
        container_shared_path(bad_path)
    with pytest.raises(ClientError):
        host_shared_path(bad_path, shared_root)


def test_upload_copies_a_file_into_shared_storage(tmp_path, shared_root):
    source = tmp_path / "book.txt"
    source.write_bytes(b"one\ntwo\n")

    exit_code, stdout, stderr = run_client(["upload", str(source), "/input/book.txt"], shared_root)

    assert exit_code == 0
    assert stderr == ""
    assert stdout == f"uploaded {source} to /input/book.txt\n"
    assert (shared_root / "input" / "book.txt").read_bytes() == b"one\ntwo\n"


def test_upload_copies_a_directory_without_overwriting(tmp_path, shared_root):
    source = tmp_path / "books"
    source.mkdir()
    (source / "a.txt").write_text("a\n", encoding="utf-8")
    (source / "b.txt").write_text("b\n", encoding="utf-8")

    assert run_client(["upload", str(source), "/input/books"], shared_root)[0] == 0
    assert (shared_root / "input" / "books" / "a.txt").read_text(encoding="utf-8") == "a\n"

    exit_code, _stdout, stderr = run_client(["upload", str(source), "/input/books"], shared_root)
    assert exit_code == 1
    assert "already exists" in stderr


def test_submit_sends_the_design_spec_over_a_real_grpc_connection(tmp_path, shared_root, client_boss):
    servicer, address = client_boss
    job_file = tmp_path / "wordcount.py"
    job_code = b"def map(key, line): yield line, 1\ndef reduce(key, values): yield key, sum(values)\n"
    job_file.write_bytes(job_code)

    exit_code, stdout, stderr = run_client(
        [
            "submit",
            "--input",
            "/input/books",
            "--output",
            "/output/wc",
            "--job",
            str(job_file),
            "--maps",
            "8",
            "--reduces",
            "4",
            "--combiner",
            "--backup-tasks",
        ],
        shared_root,
        address,
    )

    assert (exit_code, stdout, stderr) == (0, "job-42\n", "")
    assert len(servicer.submissions) == 1
    spec = servicer.submissions[0].spec
    assert spec.input_path == "/shared/input/books"
    assert spec.output_path == "/shared/output/wc"
    assert spec.job_file_name == "wordcount.py"
    assert spec.job_code == job_code
    assert (spec.num_map_tasks, spec.num_reduce_tasks) == (8, 4)
    assert (spec.use_combiner, spec.use_backup_tasks) == (True, True)


def test_build_job_spec_rejects_nonpositive_task_counts(tmp_path):
    job_file = tmp_path / "job.py"
    job_file.write_text("def map(key, line): return []\ndef reduce(key, values): return []\n", encoding="utf-8")

    with pytest.raises(ClientError, match="--maps"):
        build_job_spec("/input/a", "/output/a", job_file, 0, 1, False, False)
    with pytest.raises(ClientError, match="--reduces"):
        build_job_spec("/input/a", "/output/a", job_file, 1, 0, False, False)


def test_status_displays_progress_and_failure_reason(shared_root, client_boss):
    servicer, address = client_boss
    servicer.statuses["job-7"] = pb.JobStatus(
        job_id="job-7",
        state=pb.JOB_STATE_REDUCING,
        map_tasks_done=8,
        num_map_tasks=8,
        reduce_tasks_done=1,
        num_reduce_tasks=4,
    )
    servicer.statuses["job-8"] = pb.JobStatus(
        job_id="job-8",
        state=pb.JOB_STATE_FAILED,
        map_tasks_done=1,
        num_map_tasks=2,
        reduce_tasks_done=0,
        num_reduce_tasks=1,
        failure_reason="map task 1 failed three times",
    )

    assert run_client(["status", "job-7"], shared_root, address) == (
        0,
        "job-7: REDUCING maps 8/8, reduces 1/4\n",
        "",
    )
    assert run_client(["status", "job-8"], shared_root, address) == (
        0,
        "job-8: FAILED maps 1/2, reduces 0/1\nreason: map task 1 failed three times\n",
        "",
    )


def test_status_turns_not_found_into_a_clear_client_error(shared_root, client_boss):
    _servicer, address = client_boss

    exit_code, stdout, stderr = run_client(["status", "job-404"], shared_root, address)

    assert exit_code == 1
    assert stdout == ""
    assert "status failed (NOT_FOUND): no job with ID 'job-404'" in stderr


def test_wait_for_job_polls_once_per_second_until_success(client_boss):
    servicer, address = client_boss
    servicer.status_sequences["job-9"] = [
        pb.JobStatus(job_id="job-9", state=pb.JOB_STATE_MAPPING),
        pb.JobStatus(job_id="job-9", state=pb.JOB_STATE_SUCCEEDED),
    ]
    channel = grpc.insecure_channel(address)
    sleeps = []
    try:
        final_status = wait_for_job(pb_grpc.BossStub(channel), "job-9", sleep=sleeps.append)
    finally:
        channel.close()

    assert final_status.state == pb.JOB_STATE_SUCCEEDED
    assert sleeps == [1.0]


def test_status_wait_prints_the_final_failure_and_returns_nonzero(shared_root, client_boss):
    servicer, address = client_boss
    servicer.statuses["job-10"] = pb.JobStatus(
        job_id="job-10",
        state=pb.JOB_STATE_FAILED,
        failure_reason="map task 0 failed three times",
    )

    assert run_client(["status", "job-10", "--wait"], shared_root, address) == (
        1,
        "job-10: FAILED maps 0/0, reduces 0/0\nreason: map task 0 failed three times\n",
        "",
    )


def test_cat_streams_only_final_parts_in_reducer_order(shared_root):
    output = shared_root / "output" / "wc"
    output.mkdir(parents=True)
    (output / "part-r-00001").write_bytes(b"second\n")
    (output / "part-r-00000").write_bytes(b"first\n")
    (output / "part-r-00001.0.tmp").write_bytes(b"temporary\n")
    (output / "_events.jsonl").write_text('{"event":"job_finished"}\n', encoding="utf-8")

    captured = io.BytesIO()
    exit_code, stdout, stderr = run_client(["cat", "/output/wc"], shared_root, binary_stdout=captured)

    assert (exit_code, stdout, stderr) == (0, "", "")
    assert captured.getvalue() == b"first\nsecond\n"


def test_cat_rejects_a_directory_without_final_output(shared_root):
    (shared_root / "output" / "empty").mkdir(parents=True)

    with pytest.raises(ClientError, match="no final part-r"):
        cat_output("/output/empty", shared_root, lambda _chunk: None)
