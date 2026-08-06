# QWP vs ILP on QuestDB — TSBS ingestion (1K/4K/100K/1M) and queries

AWS `r8a.8xlarge`, latest `go-questdb-client`. All ingestion figures are **rows
sent per second** (the loader's own timer, the convention behind QuestDB's
published TSBS numbers); committed/WAL-apply rates are discussed separately.

## Summary

**Headline (4,000 hosts):** over a real network QuestDB ingests QWP at ~19M rows/s
against ILP's 5.3M (**3.6×**), and ~48M against ILP's ~11M co-located (**4.4×**).
QWP's binary rows are ~3.4× smaller on the wire, so ILP saturates the ~15 Gbit/s
link first. Reads follow the same shape — QWP's columnar batches win on large
result sets. The rest of this doc is the how and why, plus two follow-ups on the
latest client: symbol vs varchar, and the query transports.

Three findings from the symbol-vs-varchar follow-up, in order of importance:

1. **The latest Go client unblocks 1M for QWP symbol.** With the earlier client
   the 1M-host symbol run had its frames rejected by the server (connection
   dropped). Built against `go-questdb-client/v4` at the latest `main`
   (`4f2723e2`, 2026-07-30), QWP symbol now ingests 1M distinct series cleanly —
   33M rows/s co-located, 18.9M over the network.
2. **Symbol beats varchar at essentially every cardinality and topology.** The
   hypothesis that `--qwp-tags-as-varchar` would win at very high cardinality
   (flat 182 B/row vs symbol's growing frames) **did not hold**.
3. **Why:** QWP's symbol dictionary is a *session* dictionary, amortized across
   frames — not re-shipped per frame in steady state. Over a full run each
   worker's dictionary fills and later frames reference symbols by id, so symbol
   stays wire-light even at 1M. Varchar ships full tag strings on every row with
   nothing to amortize, so it is ~2× heavier on the wire at all cardinalities.

Net: with the current client, **keep SYMBOL** (the default). `--qwp-tags-as-varchar`
works and keeps the table stored as SYMBOL, but it is not a throughput win; its
one niche — a workaround for the old client's 1M frame rejection — is closed now
that symbol ingests 1M directly and about twice as fast.

## Methodology

- **Instances:** 2× AWS `r8a.8xlarge` (AMD-based, 32 vCPU, 256 GiB RAM, up to
  15 Gbps network), same availability zone (`eu-west-1c`), same VPC/subnet and
  security group so the two boxes communicate over private IPs. AMI Ubuntu 24.04
  x86_64 (`ami-08c7a4b4f234dfa77`). One box is the QuestDB server, the other is
  the loader for the network runs; the localhost runs use only the server box.
- **Storage:** 500 GB **gp3** EBS root volume per box (`DeleteOnTermination=true`),
  **20,000 IOPS / 1,000 MiB/s** — matching the published InfluxDB-comparison
  setup. Disk was not a factor regardless: the datasets and pre-encoded frames
  fit in the 256 GiB page cache, so the timed reads came from RAM, and the runs
  were CPU- and network-bound, not IOPS-bound.
- **Server:** QuestDB nightly (`73685fa`) in Docker (`questdb/questdb:nightly`),
  `--network host`. Built against `go-questdb-client/v4` `@4f2723e2` (latest
  `main`, 2026-07-30); same client on both boxes.
- **Protocols (identical data per cardinality):** ILP/TCP; QWP symbol (tags as
  SYMBOL); QWP varchar (`--qwp-tags-as-varchar`, tags sent as VARCHAR but the
  table still **stores** them as SYMBOL — every run confirmed `hostname=SYMBOL`).
  QWP figures use `--qwp-preencode-replay` (frames built outside the timed
  interval), measured at batch 10k and, for varchar, also 5k.
- **CPU pinning (both topologies):** server pinned to **30 cores**
  (`--cpuset-cpus=0-29`, pools 29). Co-located, the loader is pinned to the
  remaining **2 cores** (`taskset -c 30-31`); the loaders are light (ILP ~1.7
  cores, a replay send lighter), so two cores suffice. The server keeps the same
  30-core budget over the network, so localhost-vs-network reflects the
  transport, not a bigger server.
- **Rounds:** 2 per config; round 2 is the steady-state figure (round 1 is
  warmup). Reported numbers below are round 2.

### How the loader sends QWP

QWP is a binary, columnar protocol over a WebSocket (port 9000), from the Go
client `github.com/questdb/go-questdb-client/v4`. The loader:

- **One sender per worker** — QWP has no connection pool; each sender pipelines
  rows into an in-memory cursor engine that a background goroutine drains over
  the socket, so 32 workers open 32 senders.
- **Rows built through the typed API** — `Table()`, `Symbol(k,v)` per tag,
  `Int64Column`/`Float64Column` per field, `At(ts)`. The client encodes each
  value into the columnar wire format here. Even with the binary `questdb-qwp`
  input (which removes the *text parsing*), every row still passes through this
  encoder — that per-row encoding is the client-side CPU cost.
- **Flush per batch** (10k rows), not per row, keeping each frame under the
  server's ~2 MiB cap.
- **Publish, not commit** — `Flush` hands the batch off and returns; `Close`
  drains and waits for the final cumulative ack. `--qwp-preencode-replay` runs
  this encoder once up front, writes the frames to disk, and times only the
  replay, so the measurement is server + network with the client encoding out.

ILP does almost no client work — its text is already the wire format, so the
loader just writes bytes to a socket. That asymmetry is why an *unpinned*
co-located comparison is unfair to QWP (next).

### Why the CPU pinning: the co-located core competition

Run the loader and server on one box unpinned, and QWP and a well-tuned ILP/TCP
come out **level** on rows sent — not because the protocols are equal, but
because the QWP client's encoding steals cores from the server. Sampled from
`/proc` during a co-located 4,000-host load (100% = one core, box = 3200%):

| load | loader CPU | server CPU |
|---|---|---|
| QWP | 958% | 2006% |
| ILP/TCP | 169% | 2557% |

The QWP loader burns ~10 cores encoding rows; the ILP/TCP loader burns ~1.7 (its
text is already the wire format). On a shared box every core the QWP client takes
is one the server loses, so the two finish level despite QWP doing *less*
server-side work — per million rows/s the server actually spends ~1.8 cores on
QWP against ILP/TCP's ~2.1. **Pinning the server to a fixed 30 cores and the
light loader to the other 2 removes that confound**, and keeps the server budget
identical co-located and networked. (The CPU figures are from the original
co-located run; the client-encoding cost they measure is not client-specific.)

### ILP/TCP depends on the server's thread pools

An ILP/TCP number says as much about server configuration as about the protocol.
The nightly gives the ILP/TCP pools **2 threads** where the shared pools each get
31 (the release build has no separate ILP pool and serves TCP from the shared
ones) — re-confirmed yesterday. Left stock, ILP/TCP at 4,000 hosts sends ~1.7M
rows/s; sized to match the shared pools it reaches ~9–12M, the release-build
rate. This run sizes them to the pinned core count (`QDB_LINE_TCP_IO_WORKER_COUNT`
and `..._WRITER_WORKER_COUNT` = 29) so ILP is representative, not crippled.
Record the pool sizes with any ILP/TCP figure, or use ILP/HTTP, which rides the
shared pools and needs no tuning.

### Why with and without varchar

QWP encodes tag columns as SYMBOL using a **per-frame dictionary**: distinct tag
values are written once per frame and then referenced by id. At extreme
cardinality that dictionary grows, and with the *earlier* client the 1M-host run
produced frames the server rejected outright (it acknowledged ~9 frames, then
dropped the connection). `--qwp-tags-as-varchar` was added to test the obvious
alternative: send tags as plain VARCHAR strings — no dictionary, uniform frame
size — while keeping the table **stored** as SYMBOL. The two questions were
(1) does varchar unblock 1M, and (2) does its flat ~182 B/row beat symbol's
growing frames at high cardinality? Running both at every cardinality is what
answers them; the answer is no on both counts (see Summary) — the symbol
dictionary amortizes across frames over a full run, and the latest client
ingests 1M symbol directly.

### Data volumes: total rows vary by cardinality (important)

Row count is **not** held constant across cardinalities — a fixed window at high
cardinality would explode the dataset (1M hosts × a 2-day window ≈ 2.6 **billion**
rows), so rows-per-host is capped:

| hosts | rows/host | total rows | window (10s) |
|---|---|---|---|
| 1,000 | 17,280 | 17.3M | 2 days |
| 4,000 | 17,280 | 69.1M | 2 days |
| 100,000 | 864 | 86.4M | 2h 24m |
| 1,000,000 | 60 | 60M | 10 min |

Throughput is reported as **rows/second** (a rate), so the differing totals do
not bias the comparison; within any cardinality all three protocols load
byte-for-byte the same data.

## Results — localhost (server 30 cores, loader 2 cores)

Cells are **send rows/s (GB/s)**. GB/s = rows/s × wire bytes/row (ILP 347, QWP
varchar 182, QWP symbol ~97 amortized; GB = 10⁹ bytes).

| hosts | ILP/TCP | QWP symbol | QWP varchar (b10k) |
|---|---|---|---|
| 1K | 8.2M (2.9) | 33.8M (3.3) | 29.5M (5.4) |
| 4K | 11.1M (3.9) | **48.4M** (4.7) | 29.9M (5.4) |
| 100K | 10.3M (3.6) | 37.4M (3.6) | 15.9M (2.9) |
| 1M | 10.5M¹ (3.6) | 33.1M (3.2) | 31.5M (5.7) |

On loopback there is no bandwidth ceiling, so the load is CPU-bound and the wire
size matters less than it does over the network. Symbol leads at every scale but
peaks at 4K (48.4M) and falls to 37.4M at 100K: at 25× the distinct hostnames,
and with rows-per-host dropping from 17,280 to 864, the server pays more to
maintain and look up a larger, less cache-friendly symbol table — a real
high-cardinality cost on the ingest path that only shows when the server is the
bottleneck (it is hidden under the wire limit over the network). Varchar dips
hardest at 100K (15.9M) because sending VARCHAR into a SYMBOL column makes the
server intern those distinct hostnames on the spot.

## Results — network (server 30 cores, client on its own box)

Cells are **send rows/s (GB/s)**, same bytes/row basis as above.

| hosts | ILP/TCP | QWP symbol | QWP varchar (b10k) |
|---|---|---|---|
| 1K | 5.2M (1.8) | **18.3M** (1.8) | 10.0M (1.8) |
| 4K | 5.3M (1.8) | **19.2M** (1.9) | 9.9M (1.8) |
| 100K | 5.3M (1.8) | **19.2M** (1.9) | 9.6M (1.7) |
| 1M | 5.2M¹ (1.8) | **18.9M** (1.8) | 9.4M (1.7) |

Every column is flat across cardinality because **all three protocols saturate
the same ~1.84 GB/s (~15 Gbit/s) link** — they differ only in how many rows that
bandwidth buys:

| network | effective bytes/row | × rate | = wire |
|---|---|---|---|
| ILP | 347 | 5.3M | 1.84 GB/s |
| QWP varchar | ~182 | 10.0M | 1.82 GB/s |
| QWP symbol | ~97 (amortized) | 19.2M | ~1.85 GB/s |

ILP and varchar independently landing on 1.84 GB/s pins the NIC ceiling. Symbol
lands on the same ceiling at ~97 B/row — its session dictionary amortizes even
1M cardinality down to the ~97 B/row it costs at 4K, which is why symbol is flat
at ~19M across every cardinality. Symbol wins over the network purely because it
is the most compact encoding, packing ~2× the rows of varchar and ~3.6× of ILP
into the same 15 Gbit/s.

The **19M is the wire, not the server:** co-located (no NIC) symbol reaches
33–48M rows/s, so the server can ingest well past 19M. The network caps at 19M
because that is what ~97 B/row allows over a 15 Gbit/s link.

¹ ILP round-2 at 1M stalled on WAL backpressure (0.8M localhost / 0.8M network
vs ~10.5M / 5.2M round 1); the round-1 figure is representative.

### A note on GB/s and the symbol dictionary

Naively multiplying symbol's rows/s by a single-frame bytes/row (96 B at 4K
rising to ~318 B at 1M) overstates its wire use, because the session dictionary
amortizes those bytes away over a full run — the flat ~19M network rate, which
would be impossible at 318 B/row over a 15 Gbit/s link, is the proof. Varchar's
~182 B/row is real and constant (no dictionary), which is why varchar, not
symbol, is the one that saturates the wire.

### Send vs committed, and WAL apply

Send rate is not commit rate. QuestDB applies the write-ahead log
asynchronously, so a flushed/replayed row is *sent*, not yet queryable. This
report times **send** throughput (the convention behind QuestDB's published TSBS
numbers). Committed (rows-made-visible) rates are lower and WAL-apply-bound — and
that is the one place QWP is ahead by a wide margin: at 4,000 hosts co-located,
ILP/TCP commits ~3.5M rows/s against QWP's ~6.5M. Sweeping
`QDB_WAL_APPLY_WORKER_COUNT` (3 → 8/16/31) changed nothing, because TSBS writes a
single table and apply parallelizes per table.

## Query results (reads): pg vs http vs qwp

Separate from ingestion: the same box (r8a.8xlarge) loaded `cpu-only` scale 4000
(34.6M rows), and each of the 16 `cpu-only` query types ran **1,000 queries, 1
client worker** (QuestDB parallelizes server-side) over each query transport —
`pg` (PostgreSQL wire, the default), `http` (REST/JSON), and `qwp` (columnar
result batches). Mean latency in ms, lower is better; **bold = fastest**.

| query type | pg | http | qwp |
|---|---|---|---|
| cpu-max-all-1 | 3.53 | 2.47 | **2.35** |
| cpu-max-all-8 | 4.87 | 3.92 | **3.59** |
| cpu-max-all-32-24 | 19.82 | 17.12 | **16.45** |
| single-groupby-1-1-1 | 0.81 | 0.81 | **0.72** |
| single-groupby-1-1-12 | 2.80 | 3.09 | **2.79** |
| single-groupby-1-8-1 | 1.04 | **0.88** | 1.00 |
| single-groupby-5-1-1 | 0.80 | **0.73** | 0.77 |
| single-groupby-5-1-12 | **2.81** | 3.06 | 3.17 |
| single-groupby-5-8-1 | 1.20 | **1.07** | 1.08 |
| double-groupby-1 | **34.64** | 59.76 | 35.52 |
| double-groupby-5 | **48.52** | 115.77 | 49.65 |
| double-groupby-all | **64.14** | 175.56 | 69.18 |
| high-cpu-1 | 4.98 | 4.76 | **4.31** |
| high-cpu-all | 504.84 | 855.99 | **404.90** |
| lastpoint | 1.63 | 2.03 | **1.17** |
| groupby-orderby-limit | **4.26** | 18.21 | 4.46 |

Findings:
- **`http` (JSON) is the clear loser on wide-result queries** — 2–4× slower than
  pg/qwp on `double-groupby-*` (176ms vs ~65ms on `-all`), `groupby-orderby-limit`
  (18ms vs ~4ms) and `high-cpu-all` (856ms vs 405–505ms). Serializing many rows/
  groups to JSON is expensive; avoid `http` for analytical result sets.
- **`qwp` is at least as fast as `pg` almost everywhere, and fastest on the
  heavy scans** — the `cpu-max-all-*` family (~10–30% faster than pg),
  `high-cpu-all` (405 vs 505ms, ~20% faster than pg and 2× faster than http), and
  `lastpoint`. On the many-group `double-groupby` queries qwp and pg are level
  (both far ahead of http).
- **Tiny single-row queries are sub-millisecond on all three** and effectively
  tied — there is no result set for a columnar transport to win on.

Net, for reads: QWP's columnar batches help most exactly where the result set is
large (heavy scans / wide selects), mirroring the ingestion story; `http`/JSON is
the one to avoid for analytical results, and `pg` remains a strong default.

## Data volume

The same 69.1M rows (4,000 hosts, 2 days) are **24 GB** as ILP line-protocol text
versus **7.1 GB** as `questdb-qwp` binary — **3.4× smaller**. That ratio is the
whole ingestion story over a network: it is what lets QWP carry ~3.6× the rows of
ILP through the same ~15 Gbit/s link. It is invisible co-located, where loopback
has no bandwidth limit and quietly hands the larger text format a subsidy no real
network gives it.

## Commands — localhost run (copy-paste)

The co-located recipe on one `r8a.8xlarge`. The network runs are identical except
the loader runs on the second box, drops the `taskset`, and points at the
server's private IP (`--ilp-bind-to=<ip>:9009`, `--qwp-addr=<ip>:9000`).

**1. Start QuestDB, pinned to 30 cores (loader gets the other 2):**

```bash
sudo docker run -d --name questdb --network host --cpuset-cpus=0-29 \
  -e QDB_SHARED_WORKER_COUNT=29 \
  -e QDB_LINE_TCP_IO_WORKER_COUNT=29 \
  -e QDB_LINE_TCP_WRITER_WORKER_COUNT=29 \
  -v /home/ubuntu/qdbroot:/var/lib/questdb \
  questdb/questdb:nightly
```

**2. Build TSBS against the latest client:**

```bash
git clone --branch jv/adding_qwp https://github.com/questdb/tsbs.git
cd tsbs
go get github.com/questdb/go-questdb-client/v4@main   # resolved to 4f2723e2
go mod tidy
make tsbs_generate_data tsbs_load_questdb
BIN=$PWD/bin
```

**3. Generate data for a cardinality (both formats).** Scale 4000 shown; swap
`--scale` and `--timestamp-end` per the table:

```bash
$BIN/tsbs_generate_data --use-case=cpu-only --seed=123 --scale=4000 \
  --timestamp-start=2016-01-01T00:00:00Z --timestamp-end=2016-01-03T00:00:00Z \
  --log-interval=10s --format=questdb      --file=/home/ubuntu/data/s4000.txt

$BIN/tsbs_generate_data --use-case=cpu-only --seed=123 --scale=4000 \
  --timestamp-start=2016-01-01T00:00:00Z --timestamp-end=2016-01-03T00:00:00Z \
  --log-interval=10s --format=questdb-qwp  --file=/home/ubuntu/data/s4000.qwp
```

| hosts | `--scale` | `--timestamp-end` | rows |
|---|---|---|---|
| 1K | 1000 | 2016-01-03T00:00:00Z | 17.3M |
| 4K | 4000 | 2016-01-03T00:00:00Z | 69.1M |
| 100K | 100000 | 2016-01-01T02:24:00Z | 86.4M |
| 1M | 1000000 | 2016-01-01T00:10:00Z | 60M |

**4. ILP/TCP** (loader pinned to the 2 free cores):

```bash
curl -s -G --data-urlencode "query=drop table if exists cpu" http://127.0.0.1:9000/exec
taskset -c 30-31 $BIN/tsbs_load_questdb \
  --file=/home/ubuntu/data/s4000.txt --workers=32 --protocol=ilp
```

**5. QWP symbol** (prebuilt-frame replay, batch 10k):

```bash
curl -s -G --data-urlencode "query=drop table if exists cpu" http://127.0.0.1:9000/exec
taskset -c 30-31 $BIN/tsbs_load_questdb \
  --file=/home/ubuntu/data/s4000.qwp --workers=32 \
  --protocol=qwp --qwp-preencode-replay --batch-size=10000
```

**6. QWP varchar** (tags on the wire, SYMBOL in storage). Pre-create the table
with SYMBOL columns first so storage stays SYMBOL, then replay with
`--qwp-tags-as-varchar`:

```bash
curl -s -G --data-urlencode "query=drop table if exists cpu" http://127.0.0.1:9000/exec
curl -s -G --data-urlencode "query=create table cpu (hostname symbol, region symbol, datacenter symbol, rack symbol, os symbol, arch symbol, team symbol, service symbol, service_version symbol, service_environment symbol, usage_user long, usage_system long, usage_idle long, usage_nice long, usage_iowait long, usage_irq long, usage_softirq long, usage_steal long, usage_guest long, usage_guest_nice long, timestamp timestamp) timestamp(timestamp) partition by day wal" http://127.0.0.1:9000/exec

taskset -c 30-31 $BIN/tsbs_load_questdb \
  --file=/home/ubuntu/data/s4000.qwp --workers=32 \
  --protocol=qwp --qwp-preencode-replay --qwp-tags-as-varchar --batch-size=10000
```

For the `b5k` varchar variant, use `--batch-size=5000`.

**7. Confirm what committed, and that storage stayed SYMBOL:**

```bash
curl -s -G --data-urlencode "query=select count() from cpu" http://127.0.0.1:9000/exec
curl -s -G --data-urlencode "query=select type from table_columns('cpu') where column='hostname'" http://127.0.0.1:9000/exec
```

The loader prints its own `Summary: … mean rate … rows/sec` — that is the **send**
rate reported in the tables. `count()` reaching the expected total (and the
`hostname` type reading `SYMBOL`) is the committed / storage check. Round 1 is
warmup; take round 2. Between runs the table is dropped (symbol/ILP) or dropped
and re-created as SYMBOL (varchar).

## Caveats

- **Round 1 is warmup** (cold JIT and page cache); all headline numbers are
  round 2.
- **ILP/TCP at 1M round 2 stalled** on WAL backpressure (~0.8M vs ~5-10M round
  1); its round-1 figure is the representative one.
- **ILP/TCP depends on the server's thread pools.** The nightly gives the
  ILP/TCP pools far fewer threads than the shared pools by default, so the pools
  were sized explicitly (above) to make ILP representative rather than crippled.
- **Symbol GB/s uses the amortized ~97 B/row**, not the cold single-frame size —
  the flat ~19M network rate proves the amortization (see the note above).
- **Committed (WAL-applied) rates are much lower than send rates** for QWP
  because apply is asynchronous; this report compares **send** throughput, which
  is what the loader/replay times. WAL apply is a separate, per-table ceiling.
- **The QWP numbers are `--qwp-preencode-replay`** (server + transport, no Go
  client row-building) — a server-capacity diagnostic, not an end-to-end loader
  benchmark.
- **QWP's send rate depends on the ack policy.** In the original through-loader
  runs, plain QWP sent ~11.5M rows/s and `--qwp-await-ack` ~14.3M at 4,000 hosts
  co-located — a ~25% swing from when the client waits — so a QWP send figure
  needs the ack mode stated. The replay path used here validates the final
  cumulative ack instead.

## Reproducing

`--qwp-tags-as-varchar` and the 30/2 pinning are also documented in
`docs/questdb.md`. Raw results in this folder: `sweep-vc-localhost.json`,
`sweep-vc-network.json` (4K/100K/1M), `sweep-vc-1k-localhost.json`,
`sweep-vc-1k-network.json` (1K), and `query-results-vc.json` (the 16 query types
× pg/http/qwp).
