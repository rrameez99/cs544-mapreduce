"""Load one submitted single-file MapReduce job in an isolated module."""

import hashlib
import importlib.machinery
import importlib.util
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Callable


class JobCodeError(ValueError):
    """The submitted Python source does not implement the required interface."""


@dataclass(frozen=True)
class JobFunctions:
    """Validated functions exposed by a submitted user job."""

    map: Callable
    reduce: Callable
    combine: Callable | None


def load_job_functions(job_code: bytes, source_path: Path, require_combiner: bool) -> JobFunctions:
    """Write and execute job source through importlib, then validate its API.

    A task receives source bytes rather than an importable package. Saving an
    attempt-private source file gives tracebacks a useful filename and keeps
    separate attempts from sharing mutable module globals.
    """
    try:
        source = job_code.decode("utf-8")
    except UnicodeDecodeError as error:
        raise JobCodeError("job code must be UTF-8 Python source") from error
    if source == "":
        raise JobCodeError("job code is empty")

    try:
        source_path.write_text(source, encoding="utf-8", newline="\n")
    except OSError as error:
        raise JobCodeError(f"could not save job source at {source_path}: {error}") from error

    digest = hashlib.sha256(job_code).hexdigest()[:16]
    module_name = f"mapreduce_job_{digest}_{source_path.parent.name.replace('-', '_').replace('.', '_')}"
    loader = importlib.machinery.SourceFileLoader(module_name, str(source_path))
    spec = importlib.util.spec_from_loader(module_name, loader)
    if spec is None:
        raise JobCodeError(f"could not build an import specification for {source_path}")
    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module
    try:
        loader.exec_module(module)
    except Exception as error:
        raise JobCodeError(f"could not load user job {source_path.name}: {error}") from error
    finally:
        # Function objects keep their module globals, while later attempts get
        # a fresh module and cannot observe this attempt through sys.modules.
        sys.modules.pop(module_name, None)

    map_function = getattr(module, "map", None)
    reduce_function = getattr(module, "reduce", None)
    combine_function = getattr(module, "combine", None)
    if not callable(map_function):
        raise JobCodeError("job must define callable map(key, line)")
    if not callable(reduce_function):
        raise JobCodeError("job must define callable reduce(key, values)")
    if require_combiner and not callable(combine_function):
        raise JobCodeError("--combiner requires callable combine(key, values)")
    if combine_function is not None and not callable(combine_function):
        raise JobCodeError("combine exists but is not callable")
    return JobFunctions(map=map_function, reduce=reduce_function, combine=combine_function)
