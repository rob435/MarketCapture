# MarketCapture

Records crypto-perpetual venues' public market data to an hourly tape on disk, ships each finished hour off the host, and reads any of it back point-in-time. One process records one venue: Bybit linear or Binance USD-M. The package is `market_tape`; `python -m market_tape` and `market-tape` are its CLI.

| Venue | Feeds (`market_tape/config.py` docstring) |
| :--- | :--- |
| Bybit linear | `book:1/50/200/500/1000` (snapshot and deltas, sequence-checked), `trades`, `ticker` (funding, open interest, mark, index, turnover), `liquidations`, `kline:<interval>`, and two hourly REST lanes, `funding` (settled payments) and `account_ratio` (long/short, 5 min) |
| Binance USD-M | `book:1` (`bookTicker`), `book:5/10/20` (partial books), `trades` (`@trade`), `ticker` (mark price and 24h), `liquidations`, `kline:<interval>`, `open_interest:<seconds>` (REST poll) |

What a tape holds:

* One zstd JSON-lines segment per symbol per UTC hour, every row carrying the host's wall and monotonic receive stamps beside the venue's own (`market_tape/schema.py`).
* A coverage record per hour, saying which (symbol, feed) cells were whole and why any were not: disconnects, overruns, book gaps, shed feeds, a full disk (`market_tape/coverage.py`).
* The venue's instrument and ticker tables, hourly or daily.
* Each finished hour packed into one tar with a byte-range index, uploaded to any rclone remote, and proven by size and MD5 before the local copy may go (`market_tape/pack.py`).

Readers rebuild the book at any moment, cut bars, grade the rows' quality, range-read single symbol-hours back from the remote, and convert hours to indexed fixed-width binary blocks (`segment-*.lmtb`) that a reader seeks into. [`market_tape/README.md`](market_tape/README.md) indexes every contract; each module's docstring is its own.

## Requirements

| Need | For |
| :--- | :--- |
| Python 3.11+; Linux for a capture host | Everything; tested on Linux. There each receive stamp is the kernel's (`SO_TIMESTAMPNS`); elsewhere it is the read's |
| `zstd` on `PATH` | Recording and reading: segments are compressed and read through the command line tool |
| `rclone` | `pack`, `fetch`, and `rclone:` sources |
| `chrony` (or another disciplined clock) | A capture host: the receive stamp's error is the host clock's ([`deploy/chrony.conf`](deploy/chrony.conf)) |
| A host the venue serves | Bybit refuses some regions outright (HTTP 403 from its CDN); Binance's REST answers 451 from restricted regions while its streams still flow, so a `listed` universe there holds nothing and names must be given |

## Install

```bash
git clone https://github.com/rob435/MarketCapture && cd MarketCapture
python3 -m venv .venv
.venv/bin/pip install -e '.[fast,read]'
```

| Extra | Adds |
| :--- | :--- |
| none | `websockets`: record, pack, fetch, metrics |
| `fast` | `orjson`, the recorder's JSON on its writer thread (`status.json` `fast_json`) |
| `read` | `numpy`, `polars`, `google-crc32c`: bars, quality, block files, the rebuilt book |
| `dev` | both, plus pytest, ruff, mypy |

## Quickstart

```bash
.venv/bin/market-tape check  --config market_tape/examples/bybit-quickstart.toml
.venv/bin/market-tape record --config market_tape/examples/bybit-quickstart.toml --root ./tape
# Ctrl-C stops it. A stop leaves the open hour's segments raw; the next start compresses them.
.venv/bin/market-tape hours    ./tape
.venv/bin/market-tape rows     ./tape --hours 2026-10-02T18 | head
.venv/bin/market-tape coverage ./tape --hours 2026-10-02T18
.venv/bin/market-tape quality  ./tape --hours 2026-10-02T18
.venv/bin/market-tape bars     ./tape --hours 2026-10-02T18 --interval 60 --out bars.parquet
.venv/bin/market-tape book     ./tape --hour 2026-10-02T18 --symbol BTCUSDT
```

`market_tape/examples/binance-quickstart.toml` is the same for Binance. `market_tape/examples/bybit-full-universe.toml` records everything Bybit publishes for every listed USDT perpetual.

## A capture host

[`deploy/`](deploy) runs both recorders on one Ubuntu host under systemd, packs and uploads every hour, and samples each recorder every minute. Its configs, [`deploy/capture/bybit-linear.toml`](deploy/capture/bybit-linear.toml) and [`binance-usdm.toml`](deploy/capture/binance-usdm.toml), record every listed USDT perpetual (Bybit: 50-level book, trades, ticker, liquidations, funding, account ratio; Binance: top of book and trades). Each is commented with its measured volume and sized for a 96 GB disk shared by both and a 1 Gbps unmetered line: change `max_disk_gb`, `min_free_disk_gb` and `monthly_gb` to fit yours, then `market_tape check` it.

| Unit (`deploy/systemd/`) | Cadence | Does |
| :--- | :--- | :--- |
| `market-capture-bybit.service`, `market-capture-binance.service` | continuous | `record`, root `/var/lib/market-capture/<tape>`; `MemoryMax` is twice the config's `queue_max_mb` |
| `market-tape-pack.timer` | hourly at :10 | `pack` of both tapes to `storagebox:market-tape`, keeping 6 shipped hours locally; a tape with no root is skipped |
| `market-tape-metrics.timer` | every minute | `metrics`: each `status.json` appended to `/var/lib/market-capture/metrics/` and pushed as line protocol when `/etc/market-capture/metrics.env` names a sink |

```bash
sudo apt install python3-venv zstd rclone chrony
sudo useradd --system --user-group --no-create-home --shell /usr/sbin/nologin market-capture
sudo git clone https://github.com/rob435/MarketCapture /opt/market-capture
cd /opt/market-capture
sudo python3 -m venv .venv && sudo .venv/bin/pip install -e '.[fast]'
for config in deploy/capture/*.toml; do sudo .venv/bin/python -m market_tape check --config "$config"; done

sudo install -m 0644 deploy/chrony.conf /etc/chrony/chrony.conf && sudo systemctl restart chrony
sudo install -d -m 0750 /etc/market-capture
sudo install -m 0600 deploy/rclone.conf.example /etc/market-capture/rclone.conf   # then set host, user, key
sudo ssh-keygen -t ed25519 -N '' -f /etc/market-capture/storagebox_ed25519        # authorize the .pub on the remote
sudo install -m 0600 deploy/metrics.env.example /etc/market-capture/metrics.env   # optional sink

sudo install -m 0644 deploy/systemd/*.service deploy/systemd/*.timer /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable --now market-capture-bybit.service market-capture-binance.service
sudo systemctl enable --now market-tape-pack.timer market-tape-metrics.timer
```

A host that runs one recorder: enable only its unit, and drop the other's `--status` line from `market-tape-metrics.service`, or that recorder is sampled as absent every minute. `--keep-hours`, the remote and the roots are the units' `ExecStart` arguments.

## Tests

```bash
.venv/bin/pip install -e '.[dev]'
.venv/bin/python -m pytest -n auto
.venv/bin/ruff check . && .venv/bin/mypy
LM_TAPE_SOAK=1 .venv/bin/python -m pytest tests/market_tape/test_record_stress.py   # a burst day on the host's universe, minutes
```

The recorder tests run the real recorder against local WebSocket and REST servers; `tests/market_tape/fixtures/` holds a real Bybit hour as the recorder wrote it, its archive and its block files.

## License

[MIT](LICENSE).
