# TSBS on QuestDB: QWP vs ILP, AWS r8a.8xlarge

## Setup

| | |
|---|---|
| Instance | AWS EC2 r8a.8xlarge, 32 vCPU AMD EPYC 9R45, 247 GB RAM, eu-west-1b |
| Storage | gp3, 500 GB, 20,000 IOPS, 1000 MB/s |
| OS | Ubuntu 22.04 |
| Server | QuestDB 9.4.4-SNAPSHOT nightly (`f2c5678`) in Docker, host networking |
| Data | cpu-only, seed 123, scale 4000, 2016-01-01 to 2016-01-03, 10s interval |
| Rows | 69,120,000 (691.2M metrics) |
| Client | TSBS `jv/adding_qwp`, 32 workers, batch size 10,000, same box |

Client and server share the machine, matching the published comparison posts.

Data files, same points in both formats:

| format | size |
|---|---|
| `questdb` (line protocol text) | 23.98 GB |
| `questdb-qwp` (binary) | 7.12 GB |

## Ingestion

Server tuned with `QDB_LINE_TCP_IO_WORKER_COUNT=16` so ILP/TCP is not
constrained by the nightly's 2-thread default (see below). Three rounds,
each transport with its native format.

| transport | send rows/s | committed rows/s |
|---|---|---|
| ILP over TCP | 8.0M | ~5.0M |
| ILP over HTTP | 6.9M | ~5.3M |
| QWP | 11.3M | **6.7M** |
| QWP, ack per batch | 14.1M | 6.3M |

"send" is the loader's own timer, the convention QuestDB has published.
"committed" is wall time from loader start until the server's row count
reaches 69,120,000, i.e. rows actually visible to a client.

QWP is ~1.3x ILP on committed throughput and 1.4-1.75x on send rate. Every
transport sends faster than write-ahead log apply absorbs, so committed rates
converge on 5-7M rows/s: WAL apply is the ceiling, not the wire.

## Finding 1: the nightly's ILP/TCP default costs 5.5x

Identical hardware, data and client; 32 workers; stock config unless noted.

| server | ILP/TCP send rate |
|---|---|
| QuestDB 9.4.3 release, defaults | 9.2M rows/s |
| 9.4.4-SNAPSHOT nightly, defaults | 1.7M rows/s |
| the same nightly, 16 ILP io workers | 8.0M rows/s |

Thread pools on the nightly, 32-core box:

```
31 shared-write   31 shared-query   31 shared-network
 2 ilpio           2 ilpwriter        3 wal-apply
```

The release build has no separate `ilpio` pool at all: ILP/TCP is served by
the shared pools. Splitting the pools and leaving TCP at 2 threads is
deliberate (TCP is legacy), but with the defaults convention that TSBS
benchmarks follow, it drops the published ILP figure by 5.5x. The previously
published 8.39M rows/s reproduces on the release build.

Both ILP transports on the release build, defaults, two rounds:

| transport | send rows/s | committed rows/s |
|---|---|---|
| ILP over TCP | 9.3M / 12.5M | 3.2M / 3.9M |
| ILP over HTTP | 6.9M / 6.8M | 5.1M / 5.0M |

ILP/HTTP is the steady one: 6.8-7.0M send and ~5.0M committed on both builds,
with no tuning. ILP/TCP is the volatile one, ranging from 1.7M to 12.5M
depending on build and thread-pool settings.

It is also the transport whose send rate flatters it most. Being
fire-and-forget it outruns write-ahead log apply by the widest margin, so on
the release build it sends 1.4-1.8x faster than HTTP while committing 1.3-1.6x
slower. Whichever transport is quoted, the send rate and the committed rate
rank them differently.

Verified not to be client-side: 32 TCP connections established in every run,
and the loader reads and parses the same file at 17.3M rows/s with
`--do-load=false`.

## Finding 2: QWP send rate depends on when the client waits

Same data, same server, three rounds, split by phase:

| | publish | drain (Close) | WAL tail | total | send | committed |
|---|---|---|---|---|---|---|
| no ack | 6.00s | 0.06s | 4.20s | 10.27s | 11.5M | **6.73M** |
| ack per batch | 4.83s | 0.06s | 6.01s | 10.90s | 14.3M | 6.34M |

Awaiting acks makes publishing finish 20% sooner and leaves correspondingly
more WAL to apply after the loader exits. End to end, not awaiting is 6%
better. Nothing hides in `Close` either way (0.06s).

So a QWP "peak ingestion" number can be moved 25% by changing the ack policy
alone, while committed throughput moves the other way. Any published send rate
needs the ack policy stated next to it.

## Queries

All 16 cpu-only query types, 1000 queries each, one worker (QuestDB
parallelises queries internally, so more client workers oversubscribe the
CPU). Figures are queries/sec; the winner of each row is in bold.

| query | rows returned | pg | http | qwp |
|---|---|---|---|---|
| lastpoint | 4,000 | 630 | 496 | **916** |
| high-cpu-all | 1,596,723 | 1.02 | 0.69 | **1.45** |
| high-cpu-1 | 810 | 196 | 196 | **218** |
| double-groupby-1 | 52,000 | 30.1 | 18.6 | **30.7** |
| double-groupby-5 | 52,000 | **22.2** | 10.4 | 22.1 |
| double-groupby-all | 52,000 | **17.0** | 6.9 | 13.8 |
| groupby-orderby-limit | 5 | **123** | 40 | 110 |
| cpu-max-all-1 | 9 | 437 | **519** | 492 |
| cpu-max-all-8 | 9 | 270 | **287** | 261 |
| cpu-max-all-32-24 | 25 | 56.2 | 57.1 | **57.2** |
| single-groupby-1-1-1 | 61 | 1382 | **1446** | 1347 |
| single-groupby-1-1-12 | 720 | **655** | 567 | 566 |
| single-groupby-1-8-1 | 61 | 1003 | **1075** | 983 |
| single-groupby-5-1-1 | 61 | **1463** | 1332 | 1335 |
| single-groupby-5-1-12 | 720 | **651** | 496 | 460 |
| single-groupby-5-8-1 | 61 | 971 | **995** | 885 |

The result-set size decides the outcome:

- **Large results favour QWP.** `high-cpu-all` returns 1.6M rows per query and
  QWP runs it 2.1x faster than HTTP and 1.4x faster than the PostgreSQL wire.
  `lastpoint` returns 4,000 rows: 1.85x HTTP, 1.45x pg.
- **Small aggregate results are a wash.** Everything returning tens of rows
  lands within a few percent across all three transports, sometimes favouring
  pg, sometimes HTTP. There is no claim to make there.
- **HTTP degrades on wide results.** It is 2-3x behind on the double-groupbys
  (52,000 rows) and 3x behind on `groupby-orderby-limit`; JSON encoding is the
  cost.
- **It is not a sweep for QWP.** The PostgreSQL wire is still ahead on
  `double-groupby-all` and `groupby-orderby-limit`.

Every query type was cross-checked: the rows returned over QWP match the HTTP
response exactly, including the 1,596,723-row case, so no transport is fast by
virtue of returning less.

Note on scale: on a laptop at scale 100, `high-cpu-all` was QWP's *worst*
relative result. At scale 4000 it is its best. The columnar advantage tracks
how much data comes back, not how complex the query is.

## Reproducing

```bash
tsbs_generate_data --use-case=cpu-only --seed=123 --scale=4000 \
  --timestamp-start=2016-01-01T00:00:00Z --timestamp-end=2016-01-03T00:00:00Z \
  --log-interval=10s --format=questdb-qwp --file=/data/questdb-data.qwp

tsbs_load_questdb --file=/data/questdb-data.qwp --workers=32
```

Swap `--format=questdb` and add `--protocol=ilp-http` or `--protocol=ilp` for
the line protocol transports.

Queries, one transport at a time:

```bash
tsbs_generate_queries --use-case=cpu-only --seed=123 --scale=4000 \
  --timestamp-start=2016-01-01T00:00:00Z --timestamp-end=2016-01-03T00:00:01Z \
  --queries=1000 --format=questdb --query-type=lastpoint > /data/q-lastpoint.txt

tsbs_run_queries_questdb --file=/data/q-lastpoint.txt --workers=1 \
  --query-protocol=qwp
```

`--query-protocol` takes `pg`, `http` or `qwp`; the query files are
protocol-independent, so one set feeds all three.

Two practical notes for anyone repeating this. `high-cpu-all` dominates the
runtime, roughly 16-25 minutes per transport against 69M rows while every other
type takes seconds, so budget about 75 minutes for the full suite. And run the
harness under `nohup` with unbuffered output and incremental writes: a dropped
SSH connection during this run killed the driver, and only the final JSON dump
saved the results.
