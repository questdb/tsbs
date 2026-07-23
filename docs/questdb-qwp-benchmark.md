# TSBS on QuestDB: QWP vs ILP, AWS r8a.8xlarge

All figures are **rows sent per second**, the loader's own timer, which is the
convention behind QuestDB's published TSBS numbers. Raw results and the harness
are in [../benchmark-artifacts/](../benchmark-artifacts/).

**Short version.** With stock settings the nightly build makes ILP/TCP look
catastrophic, 1.7M rows/s against QWP's 11.1M. That is a thread-pool default,
not a protocol difference: size the ILP pools like the others and ILP/TCP
reaches 12.9M. On a fairly configured server QWP and ILP/TCP are level, and QWP
only pulls ahead with per-batch acks, by about 19% at 4,000 hosts and 5% at
100,000. Queries are where QWP wins clearly, and only when results are large.

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
Each transport reads its own format: line protocol text for ILP, the binary
`questdb-qwp` format for QWP.

## 1. Stock defaults, 4,000 hosts

69.1M rows, nightly build, nothing configured. Rows sent per second, two rounds:

| transport | round 1 | round 2 |
|---|---|---|
| ILP over TCP | 1.68M | 1.66M |
| ILP over HTTP | 7.14M | 6.94M |
| QWP | 10.50M | 11.49M |
| QWP + per-batch ack | 13.57M | 14.49M |

Read on its own this says QWP is 6-8x ILP/TCP, and that is how it looked this
morning. It is wrong, and the ILP/TCP row is the reason.

## 2. The ILP/TCP number is a thread-pool default

The nightly gives ILP/TCP its own pools and leaves them at 2 threads on a
32-core box, where everything else gets 31:

```
9.4.3 release   31 shared-write   31 shared-query   31 shared-network
                (no separate ILP pool: TCP is served by the shared pools)

9.4.4 nightly   31 shared-write   31 shared-query   31 shared-network
                 2 ilpio           2 ilpwriter
```

Same hardware, same data, same client, ILP/TCP only:

| server | rows sent |
|---|---|
| nightly, stock (2 ILP workers) | 1.7M |
| nightly, 16 ILP io + writer workers | 8.0M |
| nightly, 31 ILP io + writer workers | 9.6 / 12.9 / 12.3M |
| 9.4.3 release, stock | 9.3 / 12.5M |

Sized properly, ILP/TCP on the nightly matches the release build. The 1.7M was
never a protocol result.

Ruled out as causes: the client (32 connections verified established every run,
and the loader parses that file at 17.3M rows/s with `--do-load=false`), Docker
port mapping (host networking changed nothing), and data shape.

The published 8.39M at 4,000 hosts reproduces on the release build. It is not
reachable on the nightly with stock settings, which matters because TSBS runs
with defaults by convention: if this default ships, every published QuestDB line
protocol figure drops by 5-9x.

## 3. Fair comparison, 4,000 hosts

Same nightly server, ILP pools at 31 so ILP performs as it does on the release.
Three rounds, rows sent:

| transport | r1 | r2 | r3 | mean |
|---|---|---|---|---|
| ILP over TCP | 9.60M | 12.87M | 12.29M | 11.6M |
| QWP | 10.43M | 11.66M | 11.30M | 11.1M |
| QWP + per-batch ack | 13.97M | 13.73M | 13.67M | **13.8M** |

Plain QWP is marginally *behind* ILP/TCP. Only with per-batch acks does it lead,
by 19%. Everything above that came from ILP being hobbled.

## 4. Other scales

Rows sent. The 100 and 1,000 host runs used 16 ILP workers, so ILP is somewhat
understated there; 100,000 is shown at both 16 and 31.

| scale | rows | ILP TCP | ILP HTTP | QWP | QWP + ack |
|---|---|---|---|---|---|
| 100 | 1.7M | 2.71 / 4.60M | 3.32 / 4.81M | 2.32 / 7.77M | 7.15 / 7.33M |
| 1,000 | 17.3M | 6.52 / 6.13M | 6.96 / 7.06M | 11.00 / 10.87M | **12.88 / 13.64M** |
| 4,000 | 69.1M | 9.60 / 12.87 / 12.29M | 6.74 / 6.97M | 10.43 / 11.66 / 11.30M | **13.97 / 13.73M** |
| 100,000 (16 ILP workers) | 86.4M | 7.31 / 6.71M | 6.09 / 6.03M | **11.99 / 11.74M** | 10.46 / 9.87M |
| 100,000 (31 ILP workers) | 86.4M | 10.51 / 8.81 / 9.57M | - | **11.51 / 12.24 / 10.96M** | 9.75 / 10.25 / 10.30M |

At 100,000 hosts on the release build, stock, ILP/TCP sends 10.9 / 10.4M, and
the published figure for that scale is 11.36M, so it reproduces there too.

The 100-host row is over in under a second and measures startup rather than
throughput; the published table's 100-host figure deserves the same caveat.

Across the range, QWP leads ILP/TCP by roughly 10-20% at 1,000 and 100,000
hosts, and is level at 4,000. It is better, not dramatically so.

## 5. Write-ahead log apply workers make no difference

Worth ruling out: the server runs 3 `wal-apply` threads by default on a 32-core
box. Sweeping `QDB_WAL_APPLY_WORKER_COUNT` to 8, 16 and 31 (verified as taking
effect) changed nothing for any transport at 4,000 hosts, on send rate or
otherwise. TSBS writes a single table and apply appears to parallelise per
table, so the extra workers have nothing to share. Whether the default of 3 is
adequate for a multi-table workload is untested.

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
the PostgreSQL wire on the 1.6M-row query, 1.85x and 1.45x on `lastpoint`. On
aggregates returning tens of rows all three are within a few percent. HTTP
collapses on wide results, 2-3x behind on the double-groupbys, where JSON
encoding is the cost. The PostgreSQL wire still wins `double-groupby-all` and
`groupby-orderby-limit`.

Rows returned over QWP were cross-checked against the HTTP response for all 16
types and match exactly, including the 1,596,723-row case.

## 7. Data volume

The same 69.1M rows: 23.98 GB as line protocol text, 7.12 GB as
`questdb-qwp` binary, 3.4x smaller. Irrelevant when client and server share a
machine, potentially significant across a network, which this benchmark does not
test.

## Open questions

1. **Is the nightly's ILP/TCP thread-pool default intended to ship?** Under the
   defaults convention it costs 5-9x and invalidates the published comparisons.

2. **QWP's send rate moves 25% with the ack policy alone** (11.5M without,
   14.3M with, at 4,000 hosts). Any published QWP figure needs the ack policy
   stated beside it.

3. **Is a cross-network benchmark worth running?** QWP's 3.4x smaller wire
   footprint is invisible when client and server share a box, and every
   published TSBS number is same-host.

4. **Is 3 `wal-apply` threads adequate for multi-table workloads?** It made no
   difference here because TSBS writes one table. The `iot` use case would
   answer it.

## A note on the other metric

Every run also recorded wall time until the server's `count` reached the
expected total, i.e. when rows become visible to a client. It is not what
QuestDB publishes, so it is kept out of the tables above, but the gap is large
and consistent: at 4,000 hosts ILP/TCP commits at ~3.5M rows/s against QWP's
~6.5M at identical configuration. That is the one place QWP is ahead by a wide
margin. The figures are in `benchmark-artifacts/` if the team wants them.

## Methodology notes

- Every figure is the mean of 2-3 rounds against a table dropped and recreated
  between runs.
- The first QWP run against a freshly started server is 30-50% slow from JIT
  warm-up. Discard it, or run ILP first.
- `high-cpu-all` dominates the query suite, 16-25 minutes per transport against
  69M rows where every other type takes seconds.
- Run long jobs under `nohup` with unbuffered output and incremental result
  writes. A dropped SSH connection killed the query driver here; only the final
  JSON dump saved 75 minutes of work.
