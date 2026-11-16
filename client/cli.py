"""Host-side commands for staging input and talking to the MapReduce boss."""

import argparse
import shutil
import sys
import time
from collections.abc import Callable
from pathlib import Path, PurePosixPath
from typing import BinaryIO, TextIO

import grpc

from protos import mapreduce_pb2 as pb
from protos import mapreduce_pb2_grpc as pb_grpc

REPOSITORY_ROOT = Path(__file__).resolve().parent.parent
DEFAULT_SHARED_ROOT = REPOSITORY_ROOT / "shared"
DEFAULT_BOSS_ADDRESS = "localhost:5440"
RPC_TIMEOUT_SECONDS = 10.0
STATUS_POLL_INTERVAL_SECONDS = 1.0
COPY_CHUNK_BYTES = 1024 * 1024


class ClientError(Exception):
    """An expected command-line failure that should be shown without a traceback."""


def _virtual_path_parts(value: str) -> tuple[str, ...]:
    """Validate a client path such as ``/input/books`` and return its parts."""
    if not value.startswith("/") or value.startswith("//"):
        raise ClientError(f"shared path must start with one '/': {value!r}")
    if "\\" in value:
        raise ClientError(f"shared path must use '/' separators: {value!r}")

    path = PurePosixPath(value)
    parts = path.parts[1:]
    if not parts or any(part in (".", "..") for part in parts):
        raise ClientError(f"shared path must name a location below the shared root: {value!r}")
    if parts[0] == "shared":
        raise ClientError("client paths are already relative to /shared; use /input/... or /output/...")
    return parts


def container_shared_path(value: str) -> str:
    """Translate a client path into the container path carried over gRPC."""
    return PurePosixPath("/shared", *_virtual_path_parts(value)).as_posix()


def host_shared_path(value: str, shared_root: Path) -> Path:
    """Translate a client path into a checked path under the host shared directory."""
    root = shared_root.resolve()
    candidate = (root.joinpath(*_virtual_path_parts(value))).resolve()
    try:
        candidate.relative_to(root)
    except ValueError as error:
        raise ClientError(f"shared path escapes {root}: {value!r}") from error
    return candidate


def positive_int(value: str) -> int:
    """Parse a strictly positive command-line task count."""
    try:
        number = int(value)
    except ValueError as error:
        raise argparse.ArgumentTypeError(f"expected an integer, got {value!r}") from error
    if number < 1:
        raise argparse.ArgumentTypeError(f"must be at least 1, got {number}")
    return number


def upload(source: Path, destination: str, shared_root: Path) -> Path:
    """Copy one host file or directory to a new location in shared storage."""
    source = source.resolve()
    if not source.exists():
        raise ClientError(f"upload source does not exist: {source}")
    if not source.is_file() and not source.is_dir():
        raise ClientError(f"upload source must be a file or directory: {source}")

    target = host_shared_path(destination, shared_root)
    if target.exists() or target.is_symlink():
        raise ClientError(f"upload destination already exists: {destination}")
    try:
        target.parent.mkdir(parents=True, exist_ok=True)
        if source.is_dir():
            shutil.copytree(source, target)
        else:
            shutil.copy2(source, target)
    except OSError as error:
        raise ClientError(f"could not upload {source} to {destination}: {error}") from error
    return target


def build_job_spec(
    input_path: str,
    output_path: str,
    job_path: Path,
    num_map_tasks: int,
    num_reduce_tasks: int,
    use_combiner: bool,
    use_backup_tasks: bool,
) -> pb.JobSpec:
    """Build the design-specified JobSpec from validated client arguments."""
    if num_map_tasks < 1:
        raise ClientError(f"--maps must be at least 1, got {num_map_tasks}")
    if num_reduce_tasks < 1:
        raise ClientError(f"--reduces must be at least 1, got {num_reduce_tasks}")

    job_path = job_path.resolve()
    if not job_path.is_file():
        raise ClientError(f"job file does not exist or is not a file: {job_path}")
    try:
        job_code = job_path.read_bytes()
    except OSError as error:
        raise ClientError(f"could not read job file {job_path}: {error}") from error

    return pb.JobSpec(
        input_path=container_shared_path(input_path),
        output_path=container_shared_path(output_path),
        job_file_name=job_path.name,
        job_code=job_code,
        num_map_tasks=num_map_tasks,
        num_reduce_tasks=num_reduce_tasks,
        use_combiner=use_combiner,
        use_backup_tasks=use_backup_tasks,
    )


def submit_job(stub: pb_grpc.BossStub, spec: pb.JobSpec) -> str:
    """Submit a JobSpec and return the boss-assigned job ID."""
    try:
        response = stub.SubmitJob(pb.SubmitJobRequest(spec=spec), timeout=RPC_TIMEOUT_SECONDS)
    except grpc.RpcError as error:
        raise _rpc_error("submit", error) from error
    return response.job_id


def get_job_status(stub: pb_grpc.BossStub, job_id: str) -> pb.JobStatus:
    """Fetch one current job-status snapshot from the boss."""
    try:
        return stub.GetJobStatus(pb.GetJobStatusRequest(job_id=job_id), timeout=RPC_TIMEOUT_SECONDS)
    except grpc.RpcError as error:
        raise _rpc_error("status", error) from error


def format_status(status: pb.JobStatus) -> str:
    """Render a concise, human-readable JobStatus."""
    state = pb.JobState.Name(status.state).removeprefix("JOB_STATE_")
    summary = (
        f"{status.job_id}: {state} "
        f"maps {status.map_tasks_done}/{status.num_map_tasks}, "
        f"reduces {status.reduce_tasks_done}/{status.num_reduce_tasks}"
    )
    if status.state == pb.JOB_STATE_FAILED and status.failure_reason:
        return f"{summary}\nreason: {status.failure_reason}"
    return summary


def wait_for_job(
    stub: pb_grpc.BossStub,
    job_id: str,
    sleep: Callable[[float], object] = time.sleep,
) -> pb.JobStatus:
    """Poll a job once per second until the boss reports a terminal state."""
    while True:
        status = get_job_status(stub, job_id)
        if status.state in (pb.JOB_STATE_SUCCEEDED, pb.JOB_STATE_FAILED):
            return status
        sleep(STATUS_POLL_INTERVAL_SECONDS)


def cat_output(path: str, shared_root: Path, write: Callable[[bytes], object]) -> None:
    """Stream final reducer output parts in deterministic reducer-ID order."""
    output_directory = host_shared_path(path, shared_root)
    if not output_directory.is_dir():
        raise ClientError(f"output directory does not exist: {path}")
    try:
        part_files = sorted(
            candidate
            for candidate in output_directory.iterdir()
            if candidate.is_file()
            and candidate.name.startswith("part-r-")
            and candidate.name.removeprefix("part-r-").isdigit()
            and len(candidate.name.removeprefix("part-r-")) == 5
        )
    except OSError as error:
        raise ClientError(f"could not list output directory {path}: {error}") from error
    if not part_files:
        raise ClientError(f"output directory has no final part-r-* files: {path}")

    try:
        for part_file in part_files:
            with part_file.open("rb") as source:
                while chunk := source.read(COPY_CHUNK_BYTES):
                    write(chunk)
    except OSError as error:
        raise ClientError(f"could not read output {path}: {error}") from error


def _rpc_error(command: str, error: grpc.RpcError) -> ClientError:
    details = error.details() or "no further details from the boss"
    return ClientError(f"{command} failed ({error.code().name}): {details}")


def _with_boss(address: str, call: Callable[[pb_grpc.BossStub], object]) -> object:
    """Connect to the Compose-published boss, execute one RPC, then close the channel."""
    channel = grpc.insecure_channel(address)
    try:
        grpc.channel_ready_future(channel).result(timeout=RPC_TIMEOUT_SECONDS)
    except grpc.FutureTimeoutError as error:
        channel.close()
        raise ClientError(f"could not reach the boss at {address}") from error
    try:
        return call(pb_grpc.BossStub(channel))
    finally:
        channel.close()


def build_parser() -> argparse.ArgumentParser:
    """Construct the four-command interface specified in design.md §4."""
    parser = argparse.ArgumentParser(prog="python -m client", description="MapReduce host client")
    commands = parser.add_subparsers(dest="command", required=True)

    upload_parser = commands.add_parser("upload", help="copy a file or directory into shared storage")
    upload_parser.add_argument("source", type=Path)
    upload_parser.add_argument("destination", help="shared path such as /input/books")

    submit_parser = commands.add_parser("submit", help="submit a MapReduce job to the boss")
    submit_parser.add_argument("--input", required=True, help="shared input path such as /input/books")
    submit_parser.add_argument("--output", required=True, help="new shared output path such as /output/wc")
    submit_parser.add_argument("--job", required=True, type=Path, help="local Python map/reduce job file")
    submit_parser.add_argument("--maps", required=True, type=positive_int)
    submit_parser.add_argument("--reduces", required=True, type=positive_int)
    submit_parser.add_argument("--combiner", action="store_true", help="require and use combine()")
    submit_parser.add_argument("--backup-tasks", action="store_true", help="enable late-phase backup attempts")

    status_parser = commands.add_parser("status", help="show one job-status snapshot")
    status_parser.add_argument("job_id")
    status_parser.add_argument("--wait", action="store_true", help="poll until the job succeeds or fails")

    cat_parser = commands.add_parser("cat", help="write final reducer output parts to standard output")
    cat_parser.add_argument("path", help="shared output path such as /output/wc")

    return parser


def main(
    argv: list[str] | None = None,
    *,
    shared_root: Path = DEFAULT_SHARED_ROOT,
    boss_address: str = DEFAULT_BOSS_ADDRESS,
    stdout: TextIO | None = None,
    stderr: TextIO | None = None,
    binary_stdout: BinaryIO | None = None,
) -> int:
    """Run one client command. Dependency injection keeps command tests local and fast."""
    stdout = sys.stdout if stdout is None else stdout
    stderr = sys.stderr if stderr is None else stderr
    binary_stdout = sys.stdout.buffer if binary_stdout is None else binary_stdout
    args = build_parser().parse_args(argv)
    try:
        if args.command == "upload":
            upload(args.source, args.destination, shared_root)
            print(f"uploaded {args.source} to {args.destination}", file=stdout)
        elif args.command == "submit":
            spec = build_job_spec(
                args.input,
                args.output,
                args.job,
                args.maps,
                args.reduces,
                args.combiner,
                args.backup_tasks,
            )
            job_id = _with_boss(boss_address, lambda stub: submit_job(stub, spec))
            print(job_id, file=stdout)
        elif args.command == "status":
            if args.wait:
                status = _with_boss(boss_address, lambda stub: wait_for_job(stub, args.job_id))
            else:
                status = _with_boss(boss_address, lambda stub: get_job_status(stub, args.job_id))
            print(format_status(status), file=stdout)
            if args.wait and status.state == pb.JOB_STATE_FAILED:
                return 1
        else:
            cat_output(args.path, shared_root, binary_stdout.write)
    except ClientError as error:
        print(f"error: {error}", file=stderr)
        return 1
    return 0
