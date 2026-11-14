"""How an upload reaches the storage box: BBR on the box's routes and the watch for a crawling connection.

No test here needs root, the network or a box: `ip` and the rclone binary are
stand-in scripts, `ss`'s answers are literal samples or injected readings, and
the clock of the decision is handed in.
"""

from __future__ import annotations

import os
import subprocess
import time
from collections.abc import Callable
from pathlib import Path

import pytest

from market_tape import boxroute, crawl, pack
from market_tape.crawl import CrawlJudge, Watch
from tests.market_tape.test_pack import REMOTE, _ledger, _run, _segment, _single_tape

# What `ss -tinp` prints for a process's sockets: a header, then each
# connection as its socket's line and an indented one of `tcp_info` counters.
SS_SAMPLE = """\
State Recv-Q Send-Q Local Address:Port Peer Address:Port Process
ESTAB 0      0      127.0.0.1:34222 127.0.0.1:5432 users:(("postgres",pid=900,fd=11))
\t cubic wscale:7,7 rto:204 rtt:0.05/0.01 mss:65483 cwnd:10 bytes_sent:5000 bytes_acked:5000 bytes_received:900 segs_out:20 notsent:0 minrtt:0.03
ESTAB 0      262144 203.0.113.9:51234 192.0.2.7:23 users:(("rclone",pid=4242,fd=8))
\t cubic wscale:7,7 rto:204 rtt:3.2/1.1 mss:1448 pmtu:1500 rcvmss:536 advmss:1448 cwnd:10 ssthresh:7 bytes_sent:104857600 bytes_retrans:1048576 bytes_acked:104000000 bytes_received:5000 segs_out:73000 segs_in:2000 data_segs_out:72900 send 36.2Mbps lastsnd:12 lastrcv:3000 lastack:12 pacing_rate 72Mbps delivery_rate 4.1Mbps delivered:71800 busy:30000ms unacked:8 retrans:0/700 notsent:131072 minrtt:3
ESTAB 0      0      [2001:db8::9]:40000 [2001:db8::7]:23 users:(("rclone",pid=4243,fd=9),("ssh",pid=4244,fd=3))
\t bbr wscale:7,7 rto:204 rtt:3.0/1.0 mss:1428 cwnd:40 bytes_sent:2048 bytes_acked:2048 segs_out:3 data_segs_out:2 send 150Mbps unacked:0 minrtt:2.9
ESTAB 0      0      203.0.113.9:51300 192.0.2.8:443
\t cubic wscale:7,7 rto:204 rtt:9/4 mss:1448 cwnd:10 bytes_acked:77 notsent:5
"""


def test_ss_output_gives_each_owned_connection_its_acknowledged_and_unsent_bytes() -> None:
    owned = crawl.parse_ss(SS_SAMPLE, lambda pid: pid in (4242, 4244))

    assert owned == {
        "203.0.113.9:51234 192.0.2.7:23": (104_000_000, 131_072),
        "[2001:db8::9]:40000 [2001:db8::7]:23": (2048, 0),
    }
    # The header, a socket no process of the session holds and one `ss` could
    # name no process for are not listed.
    assert crawl.parse_ss(SS_SAMPLE, lambda pid: False) == {}
    assert crawl.parse_ss("", lambda pid: True) == {}


def test_a_counter_ss_leaves_out_reads_zero() -> None:
    sample = 'ESTAB 0 0 10.0.0.1:1 10.0.0.2:2 users:(("rclone",pid=7,fd=3))\n\t cubic rto:200 mss:1448\n'

    assert crawl.parse_ss(sample, lambda pid: pid == 7) == {"10.0.0.1:1 10.0.0.2:2": (0, 0)}


def test_a_connection_that_held_unsent_bytes_under_the_floor_through_the_window_is_a_crawl() -> None:
    judge = CrawlJudge(Watch(window=30, floor=100 * 1024))
    name = "10.0.0.1:1 192.0.2.7:23"
    # 10 KiB/s acknowledged, bytes always waiting: nothing until a whole window has passed.
    verdicts = [judge.look(float(second), {name: (10 * 1024 * second, 65_536)}) for second in range(0, 30, 5)]
    assert verdicts == [None] * 6
    why = judge.look(30.0, {name: (10 * 1024 * 30, 65_536)})
    assert why == "a connection crawled at 10 KiB/s for 30 s"


def test_a_connection_with_nothing_unsent_is_never_a_crawl() -> None:
    # The box is the one being waited on: rclone's listing, its hashing.
    judge = CrawlJudge(Watch(window=30, floor=100 * 1024))
    name = "10.0.0.1:1 192.0.2.7:23"
    for second in range(0, 600, 5):
        assert judge.look(float(second), {name: (1000, 0)}) is None


def test_a_wait_that_ends_forgets_the_time_it_had_run() -> None:
    judge = CrawlJudge(Watch(window=30, floor=100 * 1024))
    name = "10.0.0.1:1 192.0.2.7:23"
    assert judge.look(0.0, {name: (0, 100)}) is None
    assert judge.look(25.0, {name: (0, 100)}) is None
    # Drained at 28 s: the next wait counts from its own first look.
    assert judge.look(28.0, {name: (50_000, 0)}) is None
    assert judge.look(40.0, {name: (50_000, 100)}) is None
    assert judge.look(65.0, {name: (50_000, 100)}) is None
    assert judge.look(70.0, {name: (50_000, 100)}) is not None


def test_a_good_pace_is_never_stopped_however_long_it_runs() -> None:
    judge = CrawlJudge(Watch(window=30, floor=100 * 1024))
    name = "10.0.0.1:1 192.0.2.7:23"
    for second in range(0, 3600, 5):
        # 400 KiB/s with bytes always waiting: a long upload on a good connection.
        assert judge.look(float(second), {name: (400 * 1024 * second, 1 << 20)}) is None


def test_a_connection_that_vanishes_is_forgotten_and_the_watch_off_reads_nothing() -> None:
    judge = CrawlJudge(Watch(window=10, floor=1024))
    name = "10.0.0.1:1 192.0.2.7:23"
    assert judge.look(0.0, {name: (0, 100)}) is None
    assert judge.look(6.0, {}) is None
    assert judge.look(12.0, {name: (0, 100)}) is None
    assert not Watch(window=0).enabled
    assert Watch(window=30).enabled
    assert Watch(window=30).look_seconds == 5.0
    assert Watch(window=2).look_seconds == 0.5


# ---------------------------------------------------------------- the upload


def _stand_in_rclone(tmp_path: Path, hang: int) -> Path:
    """An rclone whose first `hang` calls never finish (a shell waiting on a
    `sleep` of its own, both pids kept) and whose later ones land; every call's
    arguments are logged."""

    script = tmp_path / "bin" / "rclone"
    script.parent.mkdir(exist_ok=True)
    script.write_text(
        "#!/bin/sh\n"
        f'echo "$*" >> "{tmp_path}/rclone-calls"\n'
        f'n=$(cat "{tmp_path}/count" 2>/dev/null || echo 0); n=$((n + 1)); echo $n > "{tmp_path}/count"\n'
        f"if [ $n -le {hang} ]; then\n"
        f'    echo $$ > "{tmp_path}/pid-$n"\n'
        "    sleep 600 &\n"
        f'    echo $! > "{tmp_path}/sleep-$n"\n'
        "    wait\n"
        "fi\n"
        "exit 0\n"
    )
    script.chmod(0o755)
    return script


def _gone(pid: int) -> bool:
    try:
        state = Path(f"/proc/{pid}/stat").read_text().rsplit(")", 1)[1].split()[0]
    except OSError:
        return True
    return state == "Z"


def _file(tmp_path: Path) -> Path:
    path = tmp_path / "2026-09-02T10Z.tar"
    path.write_bytes(os.urandom(4096))
    return path


def _crawling(sessions: list[int]) -> Callable[[int], dict[str, tuple[int, int]]]:
    def read(session: int) -> dict[str, tuple[int, int]]:
        sessions.append(session)
        return {"10.0.0.1:1 192.0.2.7:23": (0, 4096)}

    return read


@pytest.mark.skipif(not os.path.isdir("/proc/self"), reason="the stopped child's state is read from /proc")
def test_a_crawling_attempt_is_stopped_whole_and_the_upload_goes_again_on_a_fresh_child(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    monkeypatch.setattr(pack.shutil, "which", lambda name: f"/usr/bin/{name}")
    sessions: list[int] = []
    rclone = pack.Rclone(
        str(_stand_in_rclone(tmp_path, hang=1)),
        tmp_path / "rclone.conf",
        watch=Watch(window=0.4, floor=1024),
        connections=_crawling(sessions),
    )

    began = time.monotonic()
    rclone.upload(_file(tmp_path), REMOTE + "/x.tar")

    assert time.monotonic() - began < 30
    calls = (tmp_path / "rclone-calls").read_text().splitlines()
    assert len(calls) == 2
    for call in calls:
        # rclone's own retries stay on every attempt.
        assert "copyto" in call and "--retries 5 --low-level-retries 10" in call
    out = capsys.readouterr().out
    assert "stopped the upload of 2026-09-02T10Z.tar (attempt 1): a connection crawled at 0 KiB/s for" in out
    assert "again on a fresh connection" in out
    # The watch read the first child's own session, and the whole group is gone.
    assert sessions and set(sessions) == {int((tmp_path / "pid-1").read_text())}
    assert _gone(int((tmp_path / "pid-1").read_text()))
    assert _gone(int((tmp_path / "sleep-1").read_text()))


def test_stops_are_bounded_and_the_next_attempt_runs_unwatched_to_the_one_deadline(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    monkeypatch.setattr(pack.shutil, "which", lambda name: f"/usr/bin/{name}")
    monkeypatch.setattr(pack, "RCLONE_TIMEOUT_SECONDS", 4.0)
    sessions: list[int] = []
    rclone = pack.Rclone(
        str(_stand_in_rclone(tmp_path, hang=99)),
        tmp_path / "rclone.conf",
        watch=Watch(window=0.2, floor=1024),
        connections=_crawling(sessions),
    )

    began = time.monotonic()
    with pytest.raises(subprocess.TimeoutExpired):
        rclone.upload(_file(tmp_path), REMOTE + "/x.tar")
    took = time.monotonic() - began

    assert pack.MAX_STOPPED_ATTEMPTS == 3
    out = capsys.readouterr().out
    assert out.count("stopped the upload") == 3
    # Three stopped attempts and a fourth, never looked at, killed at the deadline measured from the start.
    assert len((tmp_path / "rclone-calls").read_text().splitlines()) == 4
    assert set(sessions) == {int((tmp_path / f"pid-{n}").read_text()) for n in (1, 2, 3)}
    assert 3.5 < took < 12
    for n in (1, 2, 3, 4):
        assert _gone(int((tmp_path / f"pid-{n}").read_text()))
        assert _gone(int((tmp_path / f"sleep-{n}").read_text()))


def test_an_attempt_that_fails_is_rclones_failure_and_not_a_stop(tmp_path: Path) -> None:
    script = tmp_path / "rclone"
    script.write_text("#!/bin/sh\nexit 7\n")
    script.chmod(0o755)
    rclone = pack.Rclone(str(script), tmp_path / "rclone.conf", watch=Watch(window=0.4, floor=1024))

    with pytest.raises(subprocess.CalledProcessError) as raised:
        rclone.upload(_file(tmp_path), REMOTE + "/x.tar")

    assert raised.value.returncode == 7


def test_without_ss_the_upload_runs_unwatched_and_says_so_once(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    monkeypatch.setattr(pack.shutil, "which", lambda name: None)

    def never(session: int) -> dict[str, tuple[int, int]]:
        raise AssertionError("a connection was read without ss")

    rclone = pack.Rclone(
        str(_stand_in_rclone(tmp_path, hang=0)),
        tmp_path / "rclone.conf",
        watch=Watch(window=0.4, floor=1024),
        connections=never,
    )
    rclone.upload(_file(tmp_path), REMOTE + "/a.tar")
    rclone.upload(_file(tmp_path), REMOTE + "/b.tar")

    captured = capsys.readouterr()
    assert captured.err.count("crawling uploads are not watched") == 1
    assert "ss is not installed" in captured.err
    assert len((tmp_path / "rclone-calls").read_text().splitlines()) == 2


def test_an_ss_that_fails_mid_upload_leaves_that_upload_unwatched_and_says_so_once(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    monkeypatch.setattr(pack.shutil, "which", lambda name: f"/usr/bin/{name}")
    script = tmp_path / "rclone"
    script.write_text("#!/bin/sh\nsleep 1\n")
    script.chmod(0o755)

    def failing(session: int) -> dict[str, tuple[int, int]]:
        raise OSError("ss exited 1")

    rclone = pack.Rclone(
        str(script), tmp_path / "rclone.conf", watch=Watch(window=0.4, floor=1024), connections=failing
    )
    rclone.upload(_file(tmp_path), REMOTE + "/a.tar")
    rclone.upload(_file(tmp_path), REMOTE + "/b.tar")

    err = capsys.readouterr().err
    assert err.count("crawling uploads are not watched") == 1
    assert "ss failed (ss exited 1)" in err


def test_a_watch_of_window_zero_reads_no_connection(tmp_path: Path) -> None:
    def never(session: int) -> dict[str, tuple[int, int]]:
        raise AssertionError("a connection was read with the watch off")

    script = tmp_path / "rclone"
    script.write_text("#!/bin/sh\nsleep 0.5\n")
    script.chmod(0o755)
    pack.Rclone(str(script), tmp_path / "rclone.conf", watch=Watch(window=0), connections=never).upload(
        _file(tmp_path), REMOTE + "/a.tar"
    )


def test_the_page_cache_is_shed_while_every_attempt_reads_the_file_and_not_after(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(pack.shutil, "which", lambda name: f"/usr/bin/{name}")
    monkeypatch.setattr(pack, "SHED_INTERVAL_SECONDS", 0.05)
    shed: list[float] = []
    monkeypatch.setattr(pack, "discard_file_cache", lambda handle: shed.append(time.monotonic()))
    rclone = pack.Rclone(
        str(_stand_in_rclone(tmp_path, hang=1)),
        tmp_path / "rclone.conf",
        watch=Watch(window=0.4, floor=1024),
        connections=_crawling([]),
    )

    rclone.upload(_file(tmp_path), REMOTE + "/x.tar")

    # The stopped first attempt alone ran several intervals, and the last drop is the upload's own.
    assert len(shed) >= 4
    ended = len(shed)
    time.sleep(0.3)
    assert len(shed) == ended


# ------------------------------------------------------------------ the routes

IP = """#!/bin/sh
echo "$*" >> "$IP_LOG"
case "$*" in
    "-o -4 route show default") printf '%s\\n' "$IP_DEFAULT_4" ;;
    "-o -6 route show default") printf '%s\\n' "$IP_DEFAULT_6" ;;
    *" route replace "*)
        if [ -n "$IP_FAIL" ]; then echo "RTNETLINK answers: Operation not permitted" >&2; exit 2; fi ;;
esac
"""
# Two IPv4 defaults (the lower metric's is the one used) and one IPv6 through a link-local gateway.
DEFAULT_4 = "default via 198.51.100.1 dev eth1 proto dhcp metric 200\ndefault via 203.0.113.1 dev eth0 proto dhcp metric 100"
DEFAULT_6 = "default via fe80::1 dev eth0 proto static metric 1024 pref medium"


def _fake_ip(tmp_path: Path, monkeypatch: pytest.MonkeyPatch, **env: str) -> Path:
    """A fake `ip` first on PATH, logging every call; the log's path."""

    shim = tmp_path / "shim"
    shim.mkdir(exist_ok=True)
    (shim / "ip").write_text(IP)
    (shim / "ip").chmod(0o755)
    log = tmp_path / "ip.log"
    values = {"IP_DEFAULT_4": DEFAULT_4, "IP_DEFAULT_6": DEFAULT_6, **env}
    for name, value in {"IP_LOG": str(log), **values}.items():
        monkeypatch.setenv(name, value)
    monkeypatch.setenv("PATH", f"{shim}{os.pathsep}{os.environ['PATH']}")
    return log


def _calls(log: Path) -> list[str]:
    return log.read_text().splitlines() if log.exists() else []


def test_each_address_of_the_box_gets_a_bbr_route_through_its_familys_default(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    log = _fake_ip(tmp_path, monkeypatch)

    routed = boxroute.route_bbr("box.example", resolver=lambda host: ["192.0.2.7", "2001:db8::7"])

    assert routed == 2
    assert _calls(log) == [
        "-o -4 route show default",
        "-4 route replace 192.0.2.7/32 via 203.0.113.1 dev eth0 onlink congctl bbr",
        "-o -6 route show default",
        "-6 route replace 2001:db8::7/128 via fe80::1 dev eth0 onlink congctl bbr",
    ]
    assert capsys.readouterr().err == ""


def test_a_failing_ip_is_said_and_does_not_stop_the_other_addresses(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    log = _fake_ip(tmp_path, monkeypatch, IP_FAIL="1")

    routed = boxroute.route_bbr("box.example", resolver=lambda host: ["192.0.2.7", "2001:db8::7"])

    assert routed == 0
    assert len([call for call in _calls(log) if " route replace " in call]) == 2
    err = capsys.readouterr().err
    assert "the route to 192.0.2.7 keeps the default congestion control" in err
    assert "the route to 2001:db8::7 keeps the default congestion control" in err
    assert "RTNETLINK answers: Operation not permitted" in err


def test_an_address_of_a_family_with_no_default_route_is_skipped(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    log = _fake_ip(tmp_path, monkeypatch, IP_DEFAULT_6="")

    routed = boxroute.route_bbr("box.example", resolver=lambda host: ["192.0.2.7", "2001:db8::7"])

    assert routed == 1
    assert [call for call in _calls(log) if " route replace " in call] == [
        "-4 route replace 192.0.2.7/32 via 203.0.113.1 dev eth0 onlink congctl bbr"
    ]
    assert capsys.readouterr().err == ""


def test_a_default_route_with_no_gateway_is_routed_by_its_device(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    log = _fake_ip(tmp_path, monkeypatch, IP_DEFAULT_4="default dev wg0 scope link")

    assert boxroute.route_bbr("box.example", resolver=lambda host: ["192.0.2.7"]) == 1
    assert _calls(log)[-1] == "-4 route replace 192.0.2.7/32 dev wg0 congctl bbr"


def test_no_ip_command_or_an_unresolved_box_sets_nothing_and_says_so(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    empty = tmp_path / "empty"
    empty.mkdir()
    monkeypatch.setenv("PATH", str(empty))
    assert boxroute.route_bbr("box.example", resolver=lambda host: ["192.0.2.7"]) == 0
    assert "no ip command" in capsys.readouterr().err

    log = _fake_ip(tmp_path, monkeypatch)
    assert boxroute.route_bbr("box.example", resolver=lambda host: []) == 0
    assert "box.example does not resolve" in capsys.readouterr().err
    assert _calls(log) == []
    assert boxroute.route_bbr("") == 0


def test_resolution_gives_each_address_once_in_byte_order_without_scoped_ones(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import socket

    def getaddrinfo(host: str, port: None, **kwargs: object) -> list[tuple[object, ...]]:
        addresses = ["192.0.2.9", "192.0.2.7", "192.0.2.7", "fe80::1%eth0", "2001:db8::7"]
        return [(socket.AF_INET, socket.SOCK_STREAM, 6, "", (address, 0)) for address in addresses]

    monkeypatch.setattr(boxroute.socket, "getaddrinfo", getaddrinfo)
    assert boxroute.resolve("box.example") == ["192.0.2.7", "192.0.2.9", "2001:db8::7"]

    def refused(*args: object, **kwargs: object) -> list[tuple[object, ...]]:
        raise socket.gaierror("no such host")

    monkeypatch.setattr(boxroute.socket, "getaddrinfo", refused)
    assert boxroute.resolve("box.example") == []


# ------------------------------------------------------------------ a run


def _pack_with_ip(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, *flags: str, hour: bool = True
) -> tuple[subprocess.CompletedProcess[str], Path]:
    """A pack run against a box that is an IP literal (resolved with no network),
    with a fake `ip` first on PATH."""

    log = _fake_ip(tmp_path, monkeypatch)
    if hour:
        _segment(tmp_path / "tape", "2026-09-02", "10", "BTCUSDT", 0)
    else:
        (tmp_path / "tape").mkdir()
    done = _run(
        tmp_path,
        *_single_tape(tmp_path),
        now="2026-09-02T11:10:00",
        box="127.0.0.1",
        flags=flags,
        env_extra={name: os.environ[name] for name in ("PATH", "IP_LOG", "IP_DEFAULT_4", "IP_DEFAULT_6", "IP_FAIL") if name in os.environ},
    )
    return done, log


def test_a_run_with_something_to_upload_routes_the_box_once_before_it_uploads(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    done, log = _pack_with_ip(tmp_path, monkeypatch)

    assert done.returncode == 0, done.stderr
    assert [call for call in _calls(log) if " route replace " in call] == [
        "-4 route replace 127.0.0.1/32 via 203.0.113.1 dev eth0 onlink congctl bbr"
    ]
    assert len(_ledger(tmp_path)) == 1


@pytest.mark.parametrize("flags", [("--no-bbr",), ("--dry-run",)])
def test_no_route_is_set_with_no_bbr_or_a_dry_run(tmp_path: Path, monkeypatch: pytest.MonkeyPatch, flags: tuple[str, ...]) -> None:
    done, log = _pack_with_ip(tmp_path, monkeypatch, *flags)

    assert done.returncode == 0, done.stderr
    assert _calls(log) == []


def test_no_route_is_set_when_nothing_is_pending(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    done, log = _pack_with_ip(tmp_path, monkeypatch, hour=False)

    assert done.returncode == 0, done.stderr
    assert _calls(log) == []


def test_a_run_whose_routes_fail_still_uploads(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("IP_FAIL", "1")
    done, log = _pack_with_ip(tmp_path, monkeypatch)
    assert done.returncode == 0, done.stderr
    assert "keeps the default congestion control" in done.stderr
    assert len(_ledger(tmp_path)) == 1


def test_the_crawl_knobs_are_refused_below_zero(tmp_path: Path) -> None:
    for flag in ("--crawl-seconds", "--crawl-floor-kibps"):
        done = _run(tmp_path, *_single_tape(tmp_path), now="2026-09-02T11:10:00", flags=(flag, "-1"))
        assert done.returncode == 2
        assert "must be zero or more" in done.stderr
