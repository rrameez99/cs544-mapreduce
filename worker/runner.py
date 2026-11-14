"""Map and reduce task execution independent of the worker gRPC process."""

import itertools
import json
import time
from collections.abc import Callable, Iterable, Iterator
from dataclasses import dataclass
from pathlib import Path

import grpc

from protos import mapreduce_pb2 as pb
from protos import mapreduce_pb2_grpc as pb_grpc
from worker.job_code import JobCodeError, JobFunctions, load_job_functions
from worker.storage import (
    DEFAULT_LOCAL_ROOT,
    DEFAULT_SHARED_ROOT,
    StorageError,
    create_private_attempt_directory,
    flush_and_fsync,
    map_attempt_directory,
    map_partition_temporary_path,
    publish_file,
    reduce_attempt_directory,
    reduce_output_paths,
    resolve_shared_path,
)

FETCH_RPC_TIMEOUT_SECONDS = 10.0


class TaskExecutionError(RuntimeError):
    """A task could not finish and should be reported to the boss."""


class UnfetchableMapOutputError(TaskExecutionError):
    """A reduce task could not read one mapper's accepted partition."""

    def __init__(self, map_task_id: int, message: str):
        super().__init__(message)
        self.map_task_id = map_task_id


@dataclass(frozen=True)
class TaskRunResult:
    """Metrics produced by a successful map or reduce attempt."""

    metrics: pb.TaskMetrics


FetchPartition = Callable[[pb.MapOutputLocation, pb.ReduceTask], Iterable[bytes]]


def partition_for_key(key: str, num_reduce_tasks: int) -> int:
    """Return the design-specified deterministic reducer partition for key."""
    if not isinstance(key, str):
        raise TaskExecutionError(f"intermediate key must be str, got {type(key).__name__}")
    if num_reduce_tasks < 1:
        raise TaskExecutionError(f"num_reduce_tasks must be positive, got {num_reduce_tasks}")
    import zlib

    return zlib.crc32(key.encode("utf-8")) % num_reduce_tasks


def _attempt_label(attempt: pb.TaskAttempt) -> str:
    kind = "map" if attempt.kind == pb.TASK_KIND_MAP else "reduce"
    return f"{attempt.job_id} {kind} task {attempt.task_id} attempt {attempt.attempt_id}"


def _validate_attempt(attempt: pb.TaskAttempt, expected_kind: int) -> None:
    if attempt.kind != expected_kind:
        raise TaskExecutionError(f"{_attempt_label(attempt)} has an invalid task kind")


def _remove_line_ending(raw_line: bytes) -> bytes:
    if raw_line.endswith(b"\n"):
        raw_line = raw_line[:-1]
        if raw_line.endswith(b"\r"):
            raw_line = raw_line[:-1]
    return raw_line


def iter_split_records(split: pb.InputSplit, shared_root: Path) -> Iterator[tuple[int, str]]:
    """Yield ``(byte_offset, line)`` records whose first byte is in split.

    The scheduler divides raw bytes. This reader discards only a line that
    began before a nonzero start offset and still reads a line that begins just
    before the end offset, matching the InputSplit contract in the proto.
    """
    if split.start_offset < 0 or split.end_offset < split.start_offset:
        raise TaskExecutionError(
            f"invalid input split {split.path!r}: [{split.start_offset}, {split.end_offset})"
        )
    try:
        input_path = resolve_shared_path(split.path, shared_root)
    except StorageError as error:
        raise TaskExecutionError(str(error)) from error
    try:
        file_size = input_path.stat().st_size
    except OSError as error:
        raise TaskExecutionError(f"could not stat input {split.path}: {error}") from error
    if split.end_offset > file_size:
        raise TaskExecutionError(
            f"input split {split.path!r} ends at {split.end_offset}, past file size {file_size}"
        )

    try:
        with input_path.open("rb") as input_file:
            if split.start_offset == 0:
                input_file.seek(0)
            elif split.start_offset == file_size:
                return
            else:
                input_file.seek(split.start_offset - 1)
                previous_byte = input_file.read(1)
                if previous_byte != b"\n":
                    input_file.readline()
            while True:
                record_offset = input_file.tell()
                if record_offset >= split.end_offset:
                    return
                raw_line = input_file.readline()
                if raw_line == b"":
                    return
                try:
                    line = _remove_line_ending(raw_line).decode("utf-8")
                except UnicodeDecodeError as error:
                    raise TaskExecutionError(
                        f"input {split.path} has invalid UTF-8 at byte {record_offset}"
                    ) from error
                yield record_offset, line
    except OSError as error:
        raise TaskExecutionError(f"could not read input {split.path}: {error}") from error


def _pair_from_user_output(value: object, function_name: str) -> tuple[str, object]:
    if not isinstance(value, (tuple, list)) or len(value) != 2:
        raise TaskExecutionError(
            f"{function_name} must yield two-item (key, value) pairs, got {value!r}"
        )
    key, output_value = value
    if not isinstance(key, str):
        raise TaskExecutionError(
            f"{function_name} yielded key of type {type(key).__name__}; keys must be str"
        )
    return key, output_value


def _encoded_intermediate_record(key: str, value: object) -> bytes:
    try:
        encoded = json.dumps([key, value], ensure_ascii=False, allow_nan=False, separators=(",", ":"))
    except (TypeError, ValueError) as error:
        raise TaskExecutionError(f"intermediate value for key {key!r} is not JSON serializable") from error
    return encoded.encode("utf-8") + b"\n"


def _user_outputs(function: Callable, function_name: str, *args: object) -> Iterator[tuple[str, object]]:
    """Invoke a user function and validate its lazily yielded key/value pairs."""
    try:
        outputs = function(*args)
        iterator = iter(outputs)
    except Exception as error:
        raise TaskExecutionError(f"{function_name} failed before yielding output: {error}") from error
    try:
        for value in iterator:
            yield _pair_from_user_output(value, function_name)
    except TaskExecutionError:
        raise
    except Exception as error:
        raise TaskExecutionError(f"{function_name} failed while yielding output: {error}") from error


class TaskRunner:
    """Runs task protobufs using local files and the worker shuffle RPC."""

    def __init__(
        self,
        local_root: Path = DEFAULT_LOCAL_ROOT,
        shared_root: Path = DEFAULT_SHARED_ROOT,
        fetch_partition: FetchPartition | None = None,
    ):
        self._local_root = local_root
        self._shared_root = shared_root
        self._fetch_partition = fetch_partition or self._fetch_partition_over_grpc

    def run_map(self, task: pb.MapTask) -> TaskRunResult:
        """Execute one map attempt and publish its complete local partitions."""
        _validate_attempt(task.attempt, pb.TASK_KIND_MAP)
        if task.num_reduce_tasks < 1:
            raise TaskExecutionError(
                f"{_attempt_label(task.attempt)} has invalid reducer count {task.num_reduce_tasks}"
            )
        started_at = time.monotonic()
        try:
            attempt_directory = map_attempt_directory(self._local_root, task.attempt)
            create_private_attempt_directory(attempt_directory)
            functions = load_job_functions(
                task.job_code,
                attempt_directory / "job.py",
                require_combiner=task.use_combiner,
            )
        except (StorageError, JobCodeError, OSError) as error:
            raise TaskExecutionError(f"{_attempt_label(task.attempt)} cannot start: {error}") from error

        records_in = 0
        partition_groups: list[dict[str, list[object]]] | None = None
        writers = self._open_map_partition_writers(task)
        records_out = 0
        bytes_written = 0
        try:
            if task.use_combiner:
                partition_groups = [dict() for _ in range(task.num_reduce_tasks)]
            for split in task.splits:
                for byte_offset, line in iter_split_records(split, self._shared_root):
                    records_in += 1
                    for key, value in _user_outputs(functions.map, "map", (split.path, byte_offset), line):
                        partition_id = partition_for_key(key, task.num_reduce_tasks)
                        if partition_groups is None:
                            encoded = _encoded_intermediate_record(key, value)
                            writers[partition_id].write(encoded)
                            records_out += 1
                            bytes_written += len(encoded)
                        else:
                            partition_groups[partition_id].setdefault(key, []).append(value)
            if partition_groups is not None:
                if functions.combine is None:  # guarded at load time; keeps type narrowing explicit
                    raise TaskExecutionError("combiner was requested but not loaded")
                for groups in partition_groups:
                    for key, values in groups.items():
                        for combined_key, combined_value in _user_outputs(
                            functions.combine, "combine", key, iter(values)
                        ):
                            partition_id = partition_for_key(combined_key, task.num_reduce_tasks)
                            encoded = _encoded_intermediate_record(combined_key, combined_value)
                            writers[partition_id].write(encoded)
                            records_out += 1
                            bytes_written += len(encoded)
            self._close_map_partition_writers(writers)
            self._publish_map_partitions(task)
        except Exception:
            self._close_map_partition_writers(writers, suppress_errors=True)
            self._remove_map_temporary_files(task)
            raise

        return TaskRunResult(
            metrics=pb.TaskMetrics(
                records_in=records_in,
                records_out=records_out,
                bytes_written=bytes_written,
                run_seconds=time.monotonic() - started_at,
            )
        )

    def run_reduce(self, task: pb.ReduceTask) -> TaskRunResult:
        """Fetch, sort, group, reduce, and atomically commit one reduce attempt."""
        _validate_attempt(task.attempt, pb.TASK_KIND_REDUCE)
        started_at = time.monotonic()
        try:
            scratch_directory = reduce_attempt_directory(self._local_root, task.attempt)
            create_private_attempt_directory(scratch_directory)
            functions = load_job_functions(task.job_code, scratch_directory / "job.py", require_combiner=False)
        except (StorageError, JobCodeError, OSError) as error:
            raise TaskExecutionError(f"{_attempt_label(task.attempt)} cannot start: {error}") from error

        records: list[tuple[str, object]] = []
        bytes_fetched = 0
        for location in task.map_outputs:
            try:
                chunks = self._fetch_partition(location, task)
                parsed_records, fetched = self._parse_partition_chunks(chunks, location.map_task_id)
            except UnfetchableMapOutputError:
                raise
            except grpc.RpcError as error:
                raise UnfetchableMapOutputError(
                    location.map_task_id,
                    f"could not fetch map task {location.map_task_id} from "
                    f"{location.fetch_address}: {error.code().name}",
                ) from error
            except OSError as error:
                raise UnfetchableMapOutputError(
                    location.map_task_id,
                    f"could not fetch map task {location.map_task_id} from {location.fetch_address}: {error}",
                ) from error
            records.extend(parsed_records)
            bytes_fetched += fetched

        records.sort(key=lambda record: record[0])
        try:
            temporary_path, final_path = reduce_output_paths(
                task.output_path, task.attempt, self._shared_root
            )
            temporary_path.parent.mkdir(parents=True, exist_ok=True)
            output_file = temporary_path.open("xb")
        except (StorageError, OSError) as error:
            raise TaskExecutionError(f"{_attempt_label(task.attempt)} cannot open output: {error}") from error

        records_out = 0
        bytes_written = 0
        try:
            for key, group in itertools.groupby(records, key=lambda record: record[0]):
                values = (value for _, value in group)
                for output_key, output_value in _user_outputs(functions.reduce, "reduce", key, values):
                    try:
                        output_text = str(output_value)
                    except Exception as error:
                        raise TaskExecutionError(
                            f"reduce output value for key {output_key!r} cannot be converted to text"
                        ) from error
                    if "\n" in output_key or "\r" in output_key or "\n" in output_text or "\r" in output_text:
                        raise TaskExecutionError("reduce output keys and values cannot contain line breaks")
                    encoded = f"{output_key}\t{output_text}\n".encode("utf-8")
                    output_file.write(encoded)
                    records_out += 1
                    bytes_written += len(encoded)
            flush_and_fsync(output_file)
            output_file.close()
            publish_file(temporary_path, final_path)
        except Exception:
            try:
                output_file.close()
            finally:
                temporary_path.unlink(missing_ok=True)
            raise

        return TaskRunResult(
            metrics=pb.TaskMetrics(
                records_in=len(records),
                records_out=records_out,
                bytes_written=bytes_written,
                bytes_fetched=bytes_fetched,
                run_seconds=time.monotonic() - started_at,
            )
        )

    def _open_map_partition_writers(self, task: pb.MapTask):
        writers = []
        try:
            for reduce_task_id in range(task.num_reduce_tasks):
                temporary_path = map_partition_temporary_path(
                    self._local_root, task.attempt, reduce_task_id
                )
                writers.append(temporary_path.open("xb"))
        except OSError as error:
            self._close_map_partition_writers(writers, suppress_errors=True)
            self._remove_map_temporary_files(task)
            raise TaskExecutionError(f"could not open map partition files: {error}") from error
        return writers

    @staticmethod
    def _close_map_partition_writers(writers, suppress_errors: bool = False) -> None:
        first_error: OSError | None = None
        for writer in writers:
            if writer.closed:
                continue
            try:
                flush_and_fsync(writer)
                writer.close()
            except OSError as error:
                try:
                    writer.close()
                except OSError:
                    pass
                if first_error is None:
                    first_error = error
        if first_error is not None and not suppress_errors:
            raise TaskExecutionError(f"could not fsync map partition: {first_error}") from first_error

    def _publish_map_partitions(self, task: pb.MapTask) -> None:
        for reduce_task_id in range(task.num_reduce_tasks):
            temporary_path = map_partition_temporary_path(self._local_root, task.attempt, reduce_task_id)
            final_path = temporary_path.with_suffix("")
            publish_file(temporary_path, final_path)

    def _remove_map_temporary_files(self, task: pb.MapTask) -> None:
        for reduce_task_id in range(task.num_reduce_tasks):
            try:
                map_partition_temporary_path(
                    self._local_root, task.attempt, reduce_task_id
                ).unlink(missing_ok=True)
            except OSError:
                # Cleanup cannot replace the original task error and affects
                # only this attempt's private temporary file.
                pass

    @staticmethod
    def _parse_partition_chunks(
        chunks: Iterable[bytes], map_task_id: int
    ) -> tuple[list[tuple[str, object]], int]:
        pending = bytearray()
        records: list[tuple[str, object]] = []
        bytes_fetched = 0
        for chunk in chunks:
            if not isinstance(chunk, bytes):
                raise TaskExecutionError(
                    f"map task {map_task_id} returned a non-bytes partition chunk"
                )
            bytes_fetched += len(chunk)
            pending.extend(chunk)
            while True:
                newline = pending.find(b"\n")
                if newline < 0:
                    break
                line = bytes(pending[:newline])
                del pending[: newline + 1]
                if line == b"":
                    raise TaskExecutionError(f"map task {map_task_id} partition contains a blank record")
                try:
                    decoded = json.loads(line.decode("utf-8"))
                except (UnicodeDecodeError, json.JSONDecodeError) as error:
                    raise TaskExecutionError(
                        f"map task {map_task_id} partition contains invalid JSONL"
                    ) from error
                key, value = _pair_from_user_output(decoded, "intermediate partition")
                records.append((key, value))
        if pending:
            raise TaskExecutionError(f"map task {map_task_id} partition ends with an incomplete JSONL record")
        return records, bytes_fetched

    @staticmethod
    def _fetch_partition_over_grpc(
        location: pb.MapOutputLocation, task: pb.ReduceTask
    ) -> Iterable[bytes]:
        channel = grpc.insecure_channel(location.fetch_address)
        stub = pb_grpc.WorkerStub(channel)
        request = pb.FetchPartitionRequest(
            job_id=task.attempt.job_id,
            map_task_id=location.map_task_id,
            attempt_id=location.attempt_id,
            reduce_task_id=task.attempt.task_id,
        )
        try:
            for response in stub.FetchPartition(request, timeout=FETCH_RPC_TIMEOUT_SECONDS):
                yield response.data
        except grpc.RpcError as error:
            raise UnfetchableMapOutputError(
                location.map_task_id,
                f"could not fetch map task {location.map_task_id} from {location.fetch_address}: {error.code().name}",
            ) from error
        finally:
            channel.close()
