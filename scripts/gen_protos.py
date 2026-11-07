"""Generate Python gRPC code from protos/mapreduce.proto.

Usage (from anywhere):
    python scripts/gen_protos.py

Writes protos/mapreduce_pb2.py, protos/mapreduce_pb2.pyi and
protos/mapreduce_pb2_grpc.py. These files are generated, so they are not
committed (see protos/.gitignore).
"""

import os
import sys
from pathlib import Path

from grpc_tools import protoc

REPO_ROOT = Path(__file__).resolve().parent.parent
PROTO_FILE = "protos/mapreduce.proto"


def main() -> None:
    # Compile from the repo root so the generated gRPC module imports
    # "protos.mapreduce_pb2" (a package import) instead of a bare
    # "mapreduce_pb2", which only works if protos/ is on sys.path.
    os.chdir(REPO_ROOT)
    exit_code = protoc.main([
        "grpc_tools.protoc",
        "--proto_path=.",
        "--python_out=.",
        "--pyi_out=.",
        "--grpc_python_out=.",
        PROTO_FILE,
    ])
    if exit_code != 0:
        sys.exit(f"protoc failed on {PROTO_FILE} (exit code {exit_code})")


if __name__ == "__main__":
    main()
