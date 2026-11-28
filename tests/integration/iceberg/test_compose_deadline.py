"""Check cleanup when the host-side Compose command hangs."""

from __future__ import annotations

import sys
from time import monotonic

from compose_deadline import supervise


def test_compose_deadline_kills_hung_command_and_cleans_scratch(tmp_path):
    scratch = tmp_path / "scratch"
    scratch.mkdir()
    marker = tmp_path / "events"
    command = tmp_path / "compose.py"
    command.write_text(
        """
import signal
import sys
from pathlib import Path
from time import sleep

marker, action, *_ = sys.argv[1:]
with Path(marker).open("a") as stream:
    stream.write(action + "\\n")
if action == "up":
    signal.signal(signal.SIGTERM, signal.SIG_IGN)
    sleep(30)
"""
    )
    started = monotonic()
    result = supervise([sys.executable, str(command), str(marker)], scratch, seconds=3, cleanup_reserve=1)
    assert result == 124
    assert marker.read_text().splitlines() == ["up", "down"]
    assert monotonic() - started < 3
    assert not scratch.exists()


def test_compose_deadline_preserves_exit_and_cleans(tmp_path):
    scratch = tmp_path / "scratch"
    scratch.mkdir()
    command = ["sh", "-c", 'if [ "$0" = up ]; then exit 17; fi']
    assert supervise(command, scratch, seconds=3, cleanup_reserve=1) == 17
    assert not scratch.exists()
