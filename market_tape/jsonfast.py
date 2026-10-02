"""The hot path's JSON: orjson when the host has it, the standard library otherwise.

The writer thread parses every frame the sockets deliver and serialises every
row it writes, and that one thread is the recorder's ceiling: past it the
queue fills, overruns, and a shard reconnects for fresh snapshots. orjson
does both jobs several times faster than the standard library. A capture
host installs it (the `fast` extra), and it is still never required: without it the recorder runs on the standard library, and says
which it found in `status.json` (`fast_json`).

Only the rows use this. Status, manifests and receipts stay on the standard
library with sorted keys, where a byte-stable file matters more than speed.
"""

from __future__ import annotations

import json
from typing import Any, Callable

try:
    import orjson
except ImportError:  # the standard library is the contract; orjson is the speed
    orjson = None  # type: ignore[assignment, unused-ignore]  # typed as a module where orjson is installed

FAST = orjson is not None

loads: Callable[[str | bytes], Any]
dumps_line: Callable[[Any], bytes]

if orjson is not None:
    _dumps = orjson.dumps
    _APPEND_NEWLINE = orjson.OPT_APPEND_NEWLINE

    # orjson appends the newline in the same buffer. A function, not
    # `functools.partial`: a partial holding a keyword builds a dict of it on
    # every call, which costs the writer more per row than this frame does.
    def _fast_dumps_line(row: Any) -> bytes:
        return _dumps(row, option=_APPEND_NEWLINE)

    loads, dumps_line = orjson.loads, _fast_dumps_line

else:

    def _loads(raw: str | bytes) -> Any:
        return json.loads(raw)

    def _dumps_line(row: Any) -> bytes:
        return json.dumps(row, separators=(",", ":")).encode() + b"\n"

    loads, dumps_line = _loads, _dumps_line
