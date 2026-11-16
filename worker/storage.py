"""Safe, attempt-scoped paths and durable file publication for workers."""

import os
import re
import time
from pathlib import Path

from protos import mapreduce_pb2 as pb

DEFAULT_LOCAL_ROOT = Path("/local")
DEFAULT_SHARED_ROOT = Path("/shared")
_SAFE_JOB_ID = re.compile(r"[A-Za-z0-9][A-Za-z0-9_.-]*\Z")


class StorageError(ValueError):
    """A task supplied an invalid path or attempt identity."""


def _require_job_id(job_id: str) -> None:
    if not _SAFE_JOB_ID.fullmatch(job_id):
        raise StorageError(f"invalid job ID {job_id!r}")


def _require_non_negative(name: str, value: int) -> None:
    if value < 0:
        raise StorageError(f"{name} must be non-negative, got {value}")


def _require_kind(attempt: pb.TaskAttempt, expected_kind: int) -> None:
    if attempt.kind != expected_kind:
        raise StorageError(
            f"expected task kind {expected_kind} for {attempt.job_id!r}, got {attempt.kind}"
        )


def _contained_path(root: Path, relative_path: Path) -> Path:
    """Return a path below root, rejecting traversal before it reaches disk."""
    resolved_root = _normalise_resolved_path(root)
    candidate = _normalise_resolved_path(resolved_root / relative_path)
    try:
        candidate.relative_to(resolved_root)
    except ValueError:
        raise StorageError(f"path {relative_path!s} escapes {resolved_root}")
    return candidate


def _normalise_resolved_path(path: Path) -> Path:
    """Resolve a path while normalising Windows' optional extended-path prefix."""
    resolved = path.resolve()
    if os.name != "nt":
        return resolved
    path_text = str(resolved)
    if path_text.startswith("\\\\?\\UNC\\"):
        return Path("\\\\" + path_text[len("\\\\?\\UNC\\") :])
    if path_text.startswith("\\\\?\\"):
        return Path(path_text[len("\\\\?\\") :])
    return resolved


def resolve_shared_path(path: str, shared_root: Path = DEFAULT_SHARED_ROOT) -> Path:
    """Map a container ``/shared/...`` path to the configured shared root.

    Tests use a temporary shared root while production workers use ``/shared``.
    The wire protocol always carries the container path, never a host path.
    """
    requested = Path(path)
    protocol_root = DEFAULT_SHARED_ROOT
    try:
        relative_path = requested.relative_to(protocol_root)
    except ValueError as error:
        raise StorageError(f"path {path!r} is not under {protocol_root}") from error
    return _contained_path(shared_root, relative_path)


def map_attempt_directory(local_root: Path, attempt: pb.TaskAttempt) -> Path:
    """Return the private local directory for one map attempt."""
    _require_kind(attempt, pb.TASK_KIND_MAP)
    _require_job_id(attempt.job_id)
    _require_non_negative("map task ID", attempt.task_id)
    _require_non_negative("attempt ID", attempt.attempt_id)
    return _contained_path(
        local_root,
        Path(attempt.job_id) / f"map-{attempt.task_id}.{attempt.attempt_id}",
    )


def reduce_attempt_directory(local_root: Path, attempt: pb.TaskAttempt) -> Path:
    """Return the local scratch directory for one reduce attempt."""
    _require_kind(attempt, pb.TASK_KIND_REDUCE)
    _require_job_id(attempt.job_id)
    _require_non_negative("reduce task ID", attempt.task_id)
    _require_non_negative("attempt ID", attempt.attempt_id)
    return _contained_path(
        local_root,
        Path(attempt.job_id) / f"reduce-{attempt.task_id}.{attempt.attempt_id}",
    )


def create_private_attempt_directory(path: Path) -> None:
    """Create an attempt directory once; reusing an attempt ID is an error."""
    path.mkdir(parents=True, exist_ok=False)


def map_partition_path(local_root: Path, attempt: pb.TaskAttempt, reduce_task_id: int) -> Path:
    """Return the published JSONL partition path for a mapper attempt."""
    _require_non_negative("reduce task ID", reduce_task_id)
    return map_attempt_directory(local_root, attempt) / f"part-{reduce_task_id}.jsonl"


def map_partition_temporary_path(local_root: Path, attempt: pb.TaskAttempt, reduce_task_id: int) -> Path:
    """Return the private temporary path used before publishing a map partition."""
    return map_partition_path(local_root, attempt, reduce_task_id).with_suffix(".jsonl.tmp")


def map_partition_path_from_request(
    local_root: Path,
    job_id: str,
    map_task_id: int,
    attempt_id: int,
    reduce_task_id: int,
) -> Path:
    """Construct a map partition path only from validated RPC request fields."""
    return map_partition_path(
        local_root,
        pb.TaskAttempt(
            job_id=job_id,
            kind=pb.TASK_KIND_MAP,
            task_id=map_task_id,
            attempt_id=attempt_id,
        ),
        reduce_task_id,
    )


def reduce_output_paths(
    output_path: str,
    attempt: pb.TaskAttempt,
    shared_root: Path = DEFAULT_SHARED_ROOT,
) -> tuple[Path, Path]:
    """Return this reduce attempt's temporary and final output paths."""
    _require_kind(attempt, pb.TASK_KIND_REDUCE)
    _require_non_negative("reduce task ID", attempt.task_id)
    _require_non_negative("attempt ID", attempt.attempt_id)
    output_directory = resolve_shared_path(output_path, shared_root)
    final_path = output_directory / f"part-r-{attempt.task_id:05d}"
    temporary_path = output_directory / f"part-r-{attempt.task_id:05d}.{attempt.attempt_id}.tmp"
    return temporary_path, final_path


def flush_and_fsync(stream) -> None:
    """Make data written to a regular file durable before publishing it."""
    stream.flush()
    os.fsync(stream.fileno())


def fsync_directory(directory: Path) -> None:
    """Persist a directory entry after an atomic rename.

    Linux containers support fsync on directories. Windows does not expose an
    equivalent directory handle, so development tests intentionally skip only
    that unavailable operation; production still executes the real fsync.
    """
    if os.name == "nt":
        return
    descriptor = os.open(directory, os.O_RDONLY)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def publish_file(temporary_path: Path, final_path: Path) -> None:
    """Atomically make a fully fsynced private file visible at final_path."""
    if os.name == "nt":
        # Concurrent replacement is atomic on the Linux deployment target. Windows can
        # transiently reject one contender, so retry briefly without hiding a persistent error.
        for retry in range(20):
            try:
                os.replace(temporary_path, final_path)
                break
            except PermissionError:
                if retry == 19:
                    raise
                time.sleep(0.01)
    else:
        os.replace(temporary_path, final_path)
    fsync_directory(final_path.parent)
