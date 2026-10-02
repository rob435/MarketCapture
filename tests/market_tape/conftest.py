from __future__ import annotations

import os
import subprocess
import threading
from typing import Any, Callable

import pytest

from market_tape import reader


class ThreadedReader:
    """`market_tape.reader` on a thread of the test's process: the same pipes
    and loop, on this interpreter, so a test's patches of the clock and the
    reader's bounds reach it. `ReaderProcess(spawn=...)` takes it in place of
    the subprocess."""

    def __init__(self, commands: int, events: int) -> None:
        self.pid: int | None = None
        self.returncode: int | None = None
        self.thread = threading.Thread(target=self._run, args=(commands, events), name="tape-reader", daemon=True)
        self.thread.start()

    def _run(self, commands: int, events: int) -> None:
        try:
            reader.Reader(commands, events).run()
        finally:
            os.close(commands)
            os.close(events)
            self.returncode = 0

    def poll(self) -> int | None:
        return None if self.thread.is_alive() else self.returncode

    def wait(self, timeout: float | None = None) -> int | None:
        self.thread.join(timeout)
        if self.thread.is_alive():
            raise subprocess.TimeoutExpired("tape-reader", timeout or 0.0)
        return self.returncode

    def kill(self) -> None:
        return None


@pytest.fixture
def reader_in_thread() -> Callable[[int, int], Any]:
    return ThreadedReader
