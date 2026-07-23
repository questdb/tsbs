# TSBS on QuestDB: QWP vs ILP, AWS r8a.8xlarge

Findings from a day of benchmarking QWP against line protocol on EC2, written
for the engineering team. Raw results and the full harness are in
[../benchmark-artifacts/](../benchmark-artifacts/).

**Short version.** On rows sent, the metric QuestDB publishes, QWP is roughly at
parity with a correctly configured ILP/TCP: marginally behind at 4,000 hosts,
about 20% ahead at 100,000, and about 19% ahead at 4,000 if per-batch acks are
enabled. The 1.4-1.7x margins we saw earlier in the day were an artefact of the
nightly build serving ILP/TCP from a 2-thread pool.

QWP's ingestion advantage is in rows made *visible*: at identical server
configuration it commits 1.85x faster than ILP/TCP, or 1.34x against the
best-tuned ILP configuration. That gap is reproducible across eight independent
runs and is not caused by write-ahead log apply starvation - raising apply
workers from 3 to 31 changes nothing for either transport, and the two do not
converge, so apply is not a shared ceiling.

ILP also carries a tuning tension that QWP does not: enlarging its thread pools
raises its send rate and lowers its committed rate.

## Setup

| | |
|---|---|
| Instance | AWS EC2 r8a.8xlarge, 32 vCPU AMD EPYC 9R45, 247 GB RAM, eu-west-1b |
| Storage | gp3, 500 GB, 20,000 IOPS, 1000 MB/s |
| OS | Ubuntu 22.04, Docker with host networking |
| Builds | QuestDB 9.4.3 release (`questdb/questdb:latest`) and 9.4.4-SNAPSHOT nightly (`f2c5678`), the only build with QWP |
| Client | TSBS `jv/adding_qwp`, 32 workers, batch size 10,000, same box as the server |
| Data | cpu-only, seed 123, 10s interval, 10 symbol columns and 10 long columns per row |

Client and server share the machine, matching the published comparison posts.

Two metrics are reported throughout:

- **sent** - the loader's own timer, which stops when the last batch has been
  handed to the transport. This is the convention behind the published figures.
- **committed** - wall time from loader start until the server's `count`
  reaches the expected total, i.e. when the rows are visible to a client.

## 1. The nightly serves ILP/TCP from a 2-thread pool

This is the finding with consequences beyond QWP.

Thread pools on a 32-core box, stock configuration:

```
9.4.3 release   31 shared-write   31 shared-query   31 shared-network   3 wal-apply
                (no separate ILP pool: TCP is served by the shared pools)

9.4.4 nightly   31 shared-write   31 shared-query   31 shared-network   3 wal-apply
                 2 ilpio           2 ilpwriter
```

ILP/TCP send rates, same hardware, data and client:

| server | 4,000 hosts | 100,000 hosts |
|---|---|---|
| 9.4.3 release, stock | 9.3 / 12.5M rows/s | 10.9 / 10.4M rows/s |
| 9.4.4 nightly, stock | 1.7M rows/s | 1.2M rows/s |
| nightly, 16 ILP io + writer workers | 8.0M rows/s | 6.3-8.3M rows/s |
| nightly, 31 ILP io + writer workers | 9.6 / 12.9 / 12.3M rows/s | 10.5 / 8.8 / 9.6M rows/s |

The nightly's stock configuration is **5.5x slower at 4,000 hosts and 8.7x
slower at 100,000** than the release. Restoring parity takes 31 io and 31 writer
workers; 16 is not enough.

Ruled out as causes: the client (32 connections were verified established in
every run, and the loader parses the same file at 17.3M rows/s with
`--do-load=false`), Docker port mapping (host networking changed nothing), and
the data shape (dense 2h24m and sparse 2-day windows at 100K differ by less than
round-to-round noise).

The published 8.39M at 4,000 hosts and 11.36M at 100,000 both reproduce on the
release build. Neither is reachable on the nightly with stock settings.

**Why this matters.** TSBS benchmarks run with default configuration by
convention. If this default ships, every published QuestDB line protocol figure
drops by 5-9x, and anyone re-running the comparison against InfluxDB or
ClickHouse will report the lower number.

## 2. QWP versus ILP on the same build

Nightly with 31 ILP io and writer workers, so ILP/TCP performs as it does on the
release. Three rounds, each transport reading its own format: line protocol text
for ILP, the binary `questdb-qwp` format for QWP.

**Rows sent:**

| scale | ILP TCP | QWP | QWP + ack |
|---|---|---|---|
| 4,000 | 9.60 / 12.87 / 12.29M | 10.43 / 11.66 / 11.30M | **13.97 / 13.73 / 13.67M** |
| 100,000 | 10.51 / 8.81 / 9.57M | **11.51 / 12.24 / 10.96M** | 9.75 / 10.25 / 10.30M |

Means: at 4,000, ILP 11.6M, QWP 11.1M, QWP+ack 13.8M. At 100,000, ILP 9.6M,
QWP 11.6M, QWP+ack 10.1M.

**Rows committed:**

| scale | ILP TCP | QWP | QWP + ack |
|---|---|---|---|
| 4,000 | 3.2-3.5M | **5.6-6.9M** | 6.4M |
| 100,000 | 3.8-4.1M | 2.9-3.6M | **5.2-5.6M** |

So on sent, QWP is at parity, and the best QWP configuration beats the best ILP
configuration by 19% at 4,000 hosts and 5% at 100,000. On committed, QWP is
1.9x at 4,000 and 1.35x at 100,000, provided acks are enabled at high
cardinality.

Note that ILP's committed rate got *worse* when its pools grew from 16 to 31
workers (about 5.0M down to 3.2-3.5M at 4,000 hosts): it sends faster and leaves
a longer write-ahead log backlog. The best ILP configuration depends on which
metric is being optimised.

## 3. The ack policy moves the send rate by 25%

QWP binary at 4,000 hosts, three rounds, split into phases:

| | publish | drain (Close) | WAL tail | total | sent | committed |
|---|---|---|---|---|---|---|
| no ack | 6.00s | 0.06s | 4.20s | 10.27s | 11.5M | **6.73M** |
| ack per batch | 4.83s | 0.06s | 6.01s | 10.90s | **14.3M** | 6.34M |

Awaiting the ack on every batch makes the publish phase finish 20% sooner and
leaves correspondingly more log to apply after the loader exits. Nothing hides
in `Close` either way. At 4,000 hosts, not awaiting is 6% better end to end.

At 100,000 hosts the relationship inverts: plain QWP commits at 2.9-3.6M and is
unstable between rounds, while awaiting acks holds 5.2-5.6M. Pacing the client
to the server is worth 1.5-1.9x at high cardinality.

A QWP send rate can therefore be moved 25% by changing when the client waits,
while committed throughput moves the other way. Any published send rate needs
the ack policy stated beside it.

## 4. Write-ahead log apply workers change nothing

Every transport commits far below what it sends, and the server runs 3
`wal-apply` threads by default on a 32-core box, so apply looked like the
obvious constraint. It is not. Sweeping `QDB_WAL_APPLY_WORKER_COUNT` with ILP
pools pinned at 31, 4,000 hosts, two rounds each, committed rows/s:

| wal-apply workers | ILP TCP | QWP | QWP + ack |
|---|---|---|---|
| 3 (default) | 3.52 / 3.27M | 6.16 / 6.86M | 6.49 / 6.16M |
| 8 | 3.39 / 3.65M | 5.78 / 6.63M | 6.36 / 6.32M |
| 16 | 3.50 / 3.69M | 6.00 / 6.76M | 6.47 / 6.49M |
| 31 | 3.52 / 3.39M | 5.86 / 6.50M | 6.35 / 6.25M |

The thread count was verified as taking effect after each restart. Ten times the
apply workers produces no change for any transport.

Two conclusions follow. First, TSBS writes a single table and apply appears to
parallelise per table, so the extra workers have one table's work to share; that
does not tell us whether the default of 3 is adequate for a customer running
many tables, which is worth testing separately with a multi-table workload.

Second, and more useful: **apply is not a shared ceiling.** At identical
configuration, on the same table and the same apply path, ILP commits at ~3.5M
and QWP at ~6.5M. If apply were the limit they would converge. The 1.85x gap is
a property of the ingestion path, not of the server being starved downstream.

## 5. Scale sweep

Committed rows/s across host counts. ILP here ran with 16 io workers, so its
figures at 4,000 and 100,000 are better read from section 2; the sweep is most
useful for the shape across scales.

| scale | rows | ILP TCP | QWP | QWP + ack |
|---|---|---|---|---|
| 100 | 1.7M | 1.92 / 2.74M | 1.73 / 3.63M | 3.48 / 3.52M |
| 1,000 | 17.3M | 4.40 / 4.82M | **6.10 / 7.36M** | 6.61 / 6.17M |
| 4,000 | 69.1M | ~5.0M | **6.7M** | 6.3M |
| 100,000 | 86.4M | 5.36 / 5.10M | 2.41 / 3.87M | **5.73 / 5.54M** |

Absolute throughput climbs with scale for every transport: at 100 hosts the
whole load is over in under a second, so that row measures startup rather than
throughput. The published table's 100-host figure deserves the same caveat.

## 6. Queries

4,000 hosts, all 16 cpu-only query types, 1000 queries each, one worker.
Queries/sec, winner in bold:

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

Result-set size decides it. QWP wins where results are large: 2.1x HTTP and 1.4x
pg on the 1.6M-row query, 1.85x and 1.45x on `lastpoint`. On aggregates
returning tens of rows all three transports are within a few percent. HTTP
collapses on wide results, 2-3x behind on the double-groupbys, where JSON
encoding is the cost. The PostgreSQL wire still wins `double-groupby-all` and
`groupby-orderby-limit`.

Rows returned over QWP were cross-checked against the HTTP response for all 16
types and match exactly, including the 1,596,723-row case.

## 7. Data volume

The same 69.1M rows, both generator formats:

| format | size |
|---|---|
| `questdb`, line protocol text | 23.98 GB |
| `questdb-qwp`, binary | 7.12 GB |

3.4x smaller on disk and on the wire. Irrelevant when client and server share a
machine, and likely significant across a real network, which this benchmark does
not test.

## Open questions for engineering

1. **Is the nightly's ILP/TCP thread-pool default intended to ship?** Under the
   defaults convention it costs 5-9x and would invalidate the published
   comparisons. If TCP is deliberately deprioritised as legacy, the published
   benchmarks need re-running over ILP/HTTP or the defaults need revisiting.

2. **Why does QWP's committed throughput collapse at 100,000 hosts without
   acks** (2.9-3.6M, unstable) **while per-batch acks hold 5.2-5.6M?** Every
   other configuration is reproducible within a few percent. A client that
   publishes 12M rows/s into a server that applies 5M rows/s is the common
   factor, but the instability specifically at high cardinality is not
   explained.

3. **Why does awaiting acks make the publish phase faster** (4.83s versus
   6.00s)? Backpressure or ring contention is the obvious guess, unverified.

4. **Why does ILP/TCP commit at half QWP's rate on the same apply path?** At
   identical configuration ILP holds ~3.5M and QWP ~6.5M, and apply workers make
   no difference to either (section 4). Both write the same table with the same
   schema, so the difference is upstream of apply, in how each transport's rows
   reach the log. This is now the most interesting open question, because it is
   where QWP's advantage actually lives.

5. **Why does ILP's committed rate fall as its pools grow** (5.0M at 16 workers
   to 3.2-3.5M at 31)? Consistent with sending further ahead of apply, but it
   means "tune for throughput" and "tune for visibility" point in opposite
   directions.

6. **Is a cross-network benchmark worth running?** QWP's 3.4x smaller wire
   footprint is invisible when client and server share a box. Published TSBS
   numbers are all same-host, so this would be new ground rather than a
   comparable figure.

7. **Is 3 `wal-apply` threads adequate for multi-table workloads?** It made no
   difference here because TSBS writes one table, but that is the case least
   likely to expose a per-table apply design. The `iot` use case, which writes
   several tables, would answer it.

## Methodology notes

- Every ingestion figure is the mean of 2-3 rounds against a table dropped and
  recreated between runs.
- Committed timings poll `select count from cpu` until it reaches the expected
  total. QuestDB applies the write-ahead log asynchronously, so a count taken
  immediately after a load always reads low.
- The first QWP run against a freshly started server is 30-50% slow from JIT
  warm-up. Discard it, or run ILP first.
- `high-cpu-all` dominates the query suite, 16-25 minutes per transport against
  69M rows where every other type takes seconds.
- Run long jobs under `nohup` with unbuffered output and incremental result
  writes. A dropped SSH connection killed the query driver here; only the final
  JSON dump saved 75 minutes of work.
