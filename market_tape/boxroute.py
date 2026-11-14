"""BBR congestion control on the routes to the storage box.

A connection to the box can lose about a segment a second for as long as it
lives, which the kernel's default (cubic) answers with tens of KiB/s; BBR paces
on the path's measured rate and round trip instead. `route_bbr` sets one host
route per address the box's host resolves to, through its family's default
route (lowest metric), with `congctl bbr`: every connection to the box takes it
and nothing else on the host does. A run derives the routes again, so they
follow a new box address or gateway. The step needs CAP_NET_ADMIN (root), and
the kernel loads `tcp_bbr` on first use.

Best effort: whatever fails is said on stderr and the upload goes on under the
default congestion control.

| Condition | Result |
| --- | --- |
| `ip route replace` succeeds for an address | that address's connections use BBR until the route is removed or the host reboots |
| no `ip` command | said once; nothing is set |
| the host does not resolve, or resolves to nothing | said; nothing is set |
| no default route of an address's family | that address is skipped, silently: the box is not reached over it |
| `ip route replace` fails (not root, no `tcp_bbr`, a refused gateway) | said with `ip`'s own words; the other addresses are still tried |

The default route's gateway is stated `onlink` always: the default route
reaches it on its device directly, but a new route naming it is refused
("Nexthop has invalid gateway") unless the kernel finds a connected route to
it, which a provider's point-to-point setup does not give.

Standard library only.
"""

from __future__ import annotations

import re
import shutil
import socket
import subprocess
import sys
from collections.abc import Callable


def _say(text: str) -> None:
    print(f"market tape: {text}", file=sys.stderr, flush=True)


def _number(text: str) -> float:
    """A route's metric word: its leading numeral, else 0."""

    found = re.match(r"[-+]?(?:[0-9]+\.?[0-9]*|\.[0-9]+)", text)
    return float(found.group(0)) if found else 0.0


def default_route(family: str) -> str:
    """The family's (`-4` or `-6`) default route of the lowest metric as `ip route`
    takes it after an address (`via GW dev DEV onlink`, or `dev DEV` with no
    gateway); "" when there is none or `ip` fails. Ties keep the first listed."""

    try:
        shown = subprocess.run(
            ["ip", "-o", family, "route", "show", "default"], stdout=subprocess.PIPE, stderr=subprocess.DEVNULL
        )
    except OSError:
        return ""
    if shown.returncode != 0:
        return ""
    best, lowest = "", 0.0
    for line in shown.stdout.decode("utf-8", "replace").splitlines():
        words = line.split()
        gateway = device = ""
        metric = 0.0
        for index in range(1, len(words)):
            following = words[index + 1] if index + 1 < len(words) else ""
            if words[index] == "via":
                gateway = following
            elif words[index] == "dev":
                device = following
            elif words[index] == "metric":
                metric = _number(following)
        if device and (not best or metric < lowest):
            lowest = metric
            best = f"via {gateway} dev {device} onlink" if gateway else f"dev {device}"
    return best


def resolve(host: str) -> list[str]:
    """Every address HOST resolves to for a stream socket, each once, in byte
    order; [] when it does not resolve. A scoped (link-local) address is left out."""

    try:
        found = socket.getaddrinfo(host, None, type=socket.SOCK_STREAM)
    except OSError:
        return []
    return sorted({str(item[4][0]) for item in found if "%" not in str(item[4][0])})


def route_bbr(host: str, *, resolver: Callable[[str], list[str]] = resolve) -> int:
    """BBR on the routes to HOST; the number of routes set. See the module docstring."""

    if not host:
        return 0
    if shutil.which("ip") is None:
        _say("no ip command; the box's connections keep the default congestion control")
        return 0
    addresses = resolver(host)
    if not addresses:
        _say(f"{host} does not resolve; the box's connections keep the default congestion control")
        return 0
    routed = 0
    for address in addresses:
        family, bits = ("-6", 128) if ":" in address else ("-4", 32)
        via = default_route(family)
        if not via:
            continue
        command = ["ip", family, "route", "replace", f"{address}/{bits}", *via.split(), "congctl", "bbr"]
        try:
            replaced = subprocess.run(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        except OSError as exc:
            _say(f"the route to {address} keeps the default congestion control: {' '.join(command)}: {exc}")
            continue
        if replaced.returncode != 0:
            detail = replaced.stderr.decode("utf-8", "replace").strip()
            _say(
                f"the route to {address} keeps the default congestion control: "
                f"{' '.join(command)} failed" + (f": {detail}" if detail else "")
            )
            continue
        routed += 1
    return routed
