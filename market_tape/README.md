# Market Tape (`market_tape`)

Records one venue's public market data per process, compresses it, ships hourly archives to the storage box, and reads any of it back point-in-time. Each module's docstring is its contract; this file indexes them and holds what spans modules.

## 1. Commands

`python -m market_tape --help` (verbs, `SOURCE` forms) and `<verb> --help`. Deployed invocations, capture host: `deploy/systemd/market-capture-bybit.service` and `market-capture-binance.service` (`record`, one venue each), `market-tape-pack.service` (`pack` of both tapes, hourly at :10, `--keep-hours 6`), `market-tape-metrics.service` (`metrics`, every minute: each `status.json` to the host record and the metrics sink). A host that reads tape but records none runs `fetch` for the hours it wants.

| Verb | Contract |
| :--- | :--- |
| `record` | `market_tape/record.py` (processes, threads, queue, budget, lock) |
| `coverage` | `market_tape/coverage.py` (cell statuses, reason codes); the recorder's account of each hour |
| `quality` | `market_tape/quality.py` (every column, the clock-step rule); the rows' own account |
| `pack` | `market_tape/pack.py` (hours, index, upload, box ledger, sliding window, recovery on an idle root) |
| `fetch` | `market_tape/fetch.py` (plan, reads, statuses, writes, window, stdout) |
| `blocks`, `index`, `rows --start-ns/--end-ns` | `market_tape/blocks.py` (§Block tape) |
| `metrics` | `market_tape/metrics.py`; the `lm_recorder` line is pinned by `tests/market_tape/fixtures/recorder_metrics_oracle.json` |

## 2. Capture config

`market_tape/config.py` docstring: tiers, feeds, universe kinds, budget and shed. Deployed: `deploy/capture/bybit-linear.toml`, `deploy/capture/binance-usdm.toml`, each commented with its reasons and measured volume; starting points: `market_tape/examples/`. `market_tape check` validates a config offline and refuses a feed the venue cannot record or more topics a connection than it allows.

## 3. Venues

| Venue | Module | Beyond its docstring |
| :--- | :--- | :--- |
| Bybit linear | `market_tape/venues/bybit.py` | `listed`: `status=Trading`, `LinearPerpetual`, `symbolType` in `market_tape/venues/bybit.py::CRYPTO_SYMBOL_TYPES`. A refused subscribe (`market_tape/venues/bybit.py::BybitAdapter::_subscribe_reply`) is a `WARNING`. Ticker: a snapshot on subscribe, then changed fields only; `funding_rate` is the running rate for the next settlement |
| Binance USD-M | `market_tape/venues/binance.py` | Field mappings: its normalizers (`market_tape/venues/binance.py::BinanceAdapter::normalize`). `fapi.binance.com` answers 451 from restricted locations while the streams still flow; without `exchangeInfo`, `listed` holds nothing and `snapshot_failures` counts |

## 4. Files

Root layout, segment lifecycle and durability: `market_tape/storage.py` docstring. Also under the root: `<day>/<HH>/_meta/coverage-<HH>-<stamp>.json.zst` (the coverage record); `manifest.jsonl`: a receipt per file the root holds (`segment_compressed`, `snapshot_compressed`, `coverage_compressed`, `segment_blocks`), its `sha256` the file's content digest, which a reader may take as the file's fingerprint, and a row per file deleted (`segment_deleted`, `snapshot_deleted`), rewritten to the live receipts on a retention pass (`market_tape/storage.py::Manifest`); `status.json` every `status_interval_seconds`. `pack` ships only `<day>/<HH>/`. Any writer that keeps this layout, one coverage record an hour, is read by every verb as a recorder root.

* `status.json`: a watchdog reads its mtime and `last_receive_ns`, measuring from `started_at_ns`. Also `dropped_frames`, `disk_dropped_frames`, `queued_*`, `shards[]` (`reanchors`, `resyncs`, `link`: the reader's last `STATS` of the connection, `market_tape/reader.py` docstring), `budget`, `compressor` (`failed`, `deferred`), `clock` (receipt less the venue's send stamp, `market_tape/record.py::Recorder::_clock_status`), `queue_wait`, `fast_json` (`orjson` present), and tier membership. The tick that writes it never walks the tape or fsyncs.
* A thin name's segment reads 0 bytes until its write buffer fills.
* Capture queue: bounded by `storage.queue_frames` and `storage.queue_max_mb`; a shard without room under both for `market_tape/reader.py::QUEUE_PUT_TIMEOUT_SECONDS` overruns (`dropped_frames`) and reconnects for fresh snapshots.
* Every Bybit book and ticker topic is re-subscribed once per UTC hour (`connection.reanchor_each_hour`), so every hour replays alone; the seam falls on funding settlement and loses one round trip of deltas. Binance's rows need none.

### Coverage record and ledger

One record per (UTC hour, recorder process), rolled at each hour boundary (written on the `tape-meta` thread) and at shutdown for the partial hour; stamp = the window's `from_ns`. Payload: `tiers[].members` spans, `shards[]`, `disconnects`, `overruns`, `book_gaps`, `shed`, `disk_blocked`, `counts`. Pruned by `retention_days` only. API: `Ledger.admissible(symbol, feed)`, `contiguous_windows` (`end` exclusive), `frame()`, `summary()`.

### Two clocks on every row

| Field or difference | Reads as |
| :--- | :--- |
| `local_receive_ts_ns` (`recv_ns`) | wall clock at kernel receive (`SO_TIMESTAMPNS` on Linux), never behind the row before it in its segment (`market_tape/storage.py::SegmentWriter::append`); the tape's sort key and replay origin |
| `local_receive_mono_ns` | same read, monotonic; one process only; 0 on side-lane rows |
| `local_receive_ts_ns - exchange_system_ts_ns` | the venue's dispatch after its send stamp, the path one way (half `status.json` `shards[].link.tcp.min_rtt_us` at least), plus host clock error |
| wall minus mono between rows | past the slew bound, a clock step (`clock_steps`, `market_tape/quality.py`) |
| both behind the row written before | read after later-stamped rows (a TCP segment held behind a retransmission, a symbol's topics on two sockets): written in read order; a replay takes it at the earlier row's stamp |

Frames from one socket read share its stamp. `exchange_system_ts_ns`: gateway send time (book, trade, ticker, liquidation, kline); `exchange_engine_ts_ns` on books; `exchange_ts_ns` on trades and liquidations.

### Book sequence contract (Bybit)

Shared by a live feed, `market_tape/venues/bybit.py` and `market_tape/record.py` (recorder), and `market_tape/book.py` (rebuild); pinned by `tests/fixtures/bybit_orderbook_sequence.jsonl`, which `tests/market_tape/test_bybit_sequence.py` reads.

| Frame on one topic | Live | Recorder | Rebuild |
| :--- | :--- | :--- | :--- |
| `type` not `delta`, or `u == 1` | re-base | `orderbook_snapshot` (`restart_snapshot` if `u == 1`) | replace |
| `u == last_u + 1` | apply | `orderbook_delta` | apply |
| other `u`, or delta before snapshot | `Resync` that topic | `sequence_gap=true`; a based topic is re-subscribed (`shards[].resyncs`), an unbased one waits for its subscription's snapshot | invalid until snapshot |
| `seq` | recorded, not a rule | `cross_sequence` | not read |

### Block tape (`market_tape.blocks`, `segment-*.lmtb`)

`blocks` converts finished hours from any `SOURCE` into one block file per symbol-hour under a new root, copying `_meta` and receipting `segment_blocks`; that root is itself a `SOURCE` with the same rows. Format: `market_tape/blocks.py` docstring. Reading needs numpy; recording never imports it.

* A read filtered by kind or receive window decodes only blocks that can hold it; a CRC failure skips the block (`source.skipped_rows`) or refuses under `--strict`. `index --verify` checks every block.
* A window read skips blocks at or past its end and trade/ticker blocks before it; book blocks are read back to the last snapshot at or before its start.

### Storage box

`<remote-base>/<tape>/YYYY/MM/DD/<day>T<HH>Z.tar` (any rclone remote that hashes; `deploy/rclone.conf.example`, installed at `/etc/market-capture/rclone.conf`; `--remote-base storagebox:market-tape` in `deploy/systemd/market-tape-pack.service`), tapes `bybit-linear` and `binance-usdm`, each tar beside its index `<day>T<HH>Z.tar.index.json` (`market_tape/pack.py::build_index`). Upload, proof and the box ledger: `market_tape/pack.py` docstring; only the current box's rows in `<state-dir>/uploaded-tapes.jsonl` license the sliding window's deletes.

### Fetch

`market_tape/fetch.py` docstring; rclone's config is `RCLONE_CONFIG`. A fetch root is its own, never a recorder root (the window, `--keep-hours` or a tape's own `--keep-hours-tape`, deletes by age alone), named after its tape so a reader infers the venue; `<state-dir>/fetch.lock` makes a second run wait within its budget. Only the planned symbols' segments and the hour's `_meta/coverage-*` are written.
