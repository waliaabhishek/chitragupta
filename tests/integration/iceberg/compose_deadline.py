"""Bound the public proof from Compose startup through resource cleanup."""

from __future__ import annotations

import os
import shutil
import signal
import subprocess
import sys
from contextlib import suppress
from pathlib import Path
from time import monotonic


class ComposeSignalError(Exception):
    def __init__(self, signum: int) -> None:
        self.signum = signum


def run_until(command: list[str], deadline: float) -> int:
    process = subprocess.Popen(command, start_new_session=True)
    try:
        return process.wait(timeout=max(0, deadline - monotonic() - 0.5))
    except subprocess.TimeoutExpired:
        return 124
    finally:
        if process.poll() is None:
            with suppress(ProcessLookupError):
                os.killpg(process.pid, signal.SIGKILL)
            process.wait(timeout=max(0.01, deadline - monotonic()))


def supervise(compose: list[str], scratch: Path, *, seconds: float = 600, cleanup_reserve: float = 20) -> int:
    deadline = monotonic() + seconds
    status = 1
    try:
        status = run_until(
            [*compose, "up", "--build", "--abort-on-container-exit", "--exit-code-from", "checks"],
            deadline - cleanup_reserve,
        )
        if status == 124:
            print("Iceberg proof reached the host deadline for Compose startup and checks.", file=sys.stderr)
    except ComposeSignalError as exc:
        status = 128 + exc.signum
    finally:
        try:
            cleanup_status = run_until([*compose, "down", "--volumes", "--remove-orphans", "-t", "3"], deadline)
            if cleanup_status:
                print(f"Iceberg proof Compose teardown failed with exit {cleanup_status}.", file=sys.stderr)
                status = cleanup_status
        finally:
            shutil.rmtree(scratch)
    return status


def interrupt(signum: int, _frame) -> None:
    raise ComposeSignalError(signum)


if __name__ == "__main__":
    signal.signal(signal.SIGINT, interrupt)
    signal.signal(signal.SIGTERM, interrupt)
    raise SystemExit(supervise(sys.argv[2:], Path(sys.argv[1])))
