"""Reading required configuration from environment variables."""

import os
import sys


def require_env(name: str) -> str:
    """Return the value of an environment variable, or exit with a clear message if it is unset or empty."""
    value = os.environ.get(name, "")
    if value == "":
        sys.exit(f"missing required environment variable {name} (set it in docker-compose.yml)")
    return value


def require_int_env(name: str) -> int:
    """Like require_env, but the value must be an integer."""
    value = require_env(name)
    try:
        return int(value)
    except ValueError:
        sys.exit(f"environment variable {name} must be an integer, got {value!r}")
