"""A stand-in rclone over a local directory, for `pack` and `fetch` tests.

`remote:path` is `$FAKE_REMOTE_DIR/path`. Every call is appended to
`$FAKE_RCLONE_LOG`. Verbs: `copyto SRC DST`, `lsjson DIR [--hash]
[--hash-type T] [--include /NAME ...] [--files-only] [--recursive]`, `cat
[--offset N] [--count N] PATH`, `about`. `--include` keeps the names its
anchored patterns match and `--hash-type` names the one hash to compute, as
rclone's own do; with `$FAKE_RCLONE_HASH_LOG` set, every file a listing
hashes is appended to it, which is what the box reads for a listing.
A missing directory or file exits 3, as rclone does. `FAKE_RCLONE_CORRUPT=1`
stores garbage for every `copyto`, `=index` for index sidecars only;
`FAKE_RCLONE_CAT_SLEEP` delays every `cat` by that many seconds. A
`--bwlimit=`, `--buffer-size=` or `--sftp-…` flag is logged and otherwise
ignored.

`install(directory)` writes an executable `rclone` there that runs this file.
"""

from __future__ import annotations

import fnmatch
import hashlib
import json
import os
import shutil
import sys
import time
from pathlib import Path


def install(directory: Path) -> Path:
    executable = directory / "rclone"
    executable.write_text(f'#!/bin/sh\nexec "{sys.executable}" "{Path(__file__).resolve()}" "$@"\n', encoding="utf-8")
    executable.chmod(0o755)
    return executable


def _local(remote: str) -> str:
    return os.path.join(os.environ["FAKE_REMOTE_DIR"], remote.split(":", 1)[1].lstrip("/"))


def _option(args: list[str], name: str) -> int | None:
    if name not in args:
        return None
    at = args.index(name)
    value = int(args[at + 1])
    del args[at : at + 2]
    return value


def main(argv: list[str]) -> int:
    args = list(argv)
    with open(os.environ["FAKE_RCLONE_LOG"], "a", encoding="utf-8") as log:
        log.write(" ".join(args) + "\n")
    if "--config" in args:
        at = args.index("--config")
        del args[at : at + 2]
    args = [arg for arg in args if not arg.startswith(("--bwlimit=", "--buffer-size=", "--sftp-"))]
    command = args[0]
    if command == "copyto":
        target = _local(args[2])
        os.makedirs(os.path.dirname(target), exist_ok=True)
        corrupt = os.environ.get("FAKE_RCLONE_CORRUPT", "0")
        if corrupt == "1" or (corrupt == "index" and target.endswith(".index.json")):
            with open(target, "wb") as handle:
                handle.write(b"not what was sent")
        else:
            shutil.copyfile(args[1], target)
        return 0
    if command == "lsjson":
        directory = _local(args[1])
        if not os.path.isdir(directory):
            print(f"error listing: directory not found: {args[1]}", file=sys.stderr)
            return 3
        includes = [args[at + 1] for at, arg in enumerate(args) if arg == "--include"]
        hash_types = [args[at + 1] for at, arg in enumerate(args) if arg == "--hash-type"]
        hashed = "--hash" in args or bool(hash_types)
        rows = []
        for name in sorted(os.listdir(directory)):
            path = os.path.join(directory, name)
            if not os.path.isfile(path):
                continue
            if includes and not any(fnmatch.fnmatchcase(f"/{name}", pattern) for pattern in includes):
                continue
            row: dict[str, object] = {"Path": name, "Name": name, "Size": os.path.getsize(path)}
            if hashed and (not hash_types or "md5" in hash_types):
                with open(path, "rb") as handle:
                    row["Hashes"] = {"md5": hashlib.md5(handle.read()).hexdigest()}
                if os.environ.get("FAKE_RCLONE_HASH_LOG"):
                    with open(os.environ["FAKE_RCLONE_HASH_LOG"], "a", encoding="utf-8") as log:
                        log.write(name + "\n")
            rows.append(row)
        print(json.dumps(rows))
        return 0
    if command == "cat":
        delay = float(os.environ.get("FAKE_RCLONE_CAT_SLEEP") or 0)
        if delay:
            time.sleep(delay)
        offset = _option(args, "--offset") or 0
        count = _option(args, "--count")
        path = _local(args[1])
        if not os.path.isfile(path):
            print(f"error: object not found: {args[1]}", file=sys.stderr)
            return 3
        with open(path, "rb") as handle:
            handle.seek(offset)
            data = handle.read() if count is None or count < 0 else handle.read(count)
        sys.stdout.buffer.write(data)
        return 0
    if command == "about":
        print(json.dumps({"total": 5 * 1024**4, "used": 15 * 1024**3, "free": 4 * 1024**4}))
        return 0
    print(f"fake rclone: no verb {command}", file=sys.stderr)
    return 1


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
