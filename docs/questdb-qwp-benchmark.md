# TSBS on QuestDB: QWP vs ILP, AWS r8a.8xlarge

All figures are **rows sent per second**, the loader's own timer, which is the
convention behind QuestDB's published TSBS numbers. Raw results and the harness
are in [../benchmark-artifacts/](../benchmark-artifacts/).

**Short version.** With client and server on the same box, QWP and a
well-configured ILP/TCP are roughly level on rows sent, because both share the
32 cores and QWP's client-side encoding competes with the server for them. Put
the client on its own instance, which is how a database is actually run, and the
picture separates cleanly: **ILP saturates the network at 5.3M rows/s while QWP
sustains 9-12M**, 1.7-2.2x faster, because line protocol text is 3.4x larger on
the wire. That is the result that matters, and it is in section 1.

The rest of the document is how we got there: a nightly thread-pool default that
made ILP/TCP look catastrophic until corrected, the same-host numbers where the
two look level, the ack policy, a scale sweep, and queries.

## Setup

| | |
|---|---|
| Instance | AWS EC2 r8a.8xlarge, 32 vCPU AMD EPYC 9R45, 247 GB RAM, eu-west-1b |
| Storage | gp3, 500 GB, 20,000 IOPS, 1000 MB/s |
| OS | Ubuntu 22.04, Docker with host networking |
| Builds | QuestDB 9.4.3 release (`questdb/questdb:latest`) and 9.4.4-SNAPSHOT nightly (`f2c5678`), the only build with QWP |
| Client | TSBS `jv/adding_qwp`, 32 workers, batch size 10,000, same box as the server |
| Data | cpu-only, seed 123, 10s interval, 10 symbol columns and 10 long columns per row |

Each transport reads its own format: line protocol text for ILP, the binary
`questdb-qwp` format for QWP. Sections 3 onward have client and server on one
box, matching the published comparison posts. Section 1 splits them onto two
instances, and section 2 isolates where QWP's bottleneck sits.

### How the loader sends QWP

QWP is the QuestDB Wire Protocol: a binary, columnar protocol spoken over a
WebSocket on port 9000, provided by the Go client
(`github.com/questdb/go-questdb-client/v4`). The loader uses it like this:

- **One sender per worker.** QWP has no connection pool. A single sender already
  pipelines: it appends rows into an in-memory cursor engine and a background
  goroutine delivers them over the WebSocket. So for 32 workers the loader opens
  32 independent senders, each `ws::addr=HOST:9000;auto_flush=off;`, rather than
  pooling one.
- **Rows are built through the client's typed API.** For every point the loader
  calls `Table(name)`, then `Symbol(k, v)` for each tag, then `Int64Column` /
  `Float64Column` for each field, then `At(ts)` to close the row. The client
  encodes each value into the columnar wire format at this point. This is true
  even when the input is the binary `questdb-qwp` file: that format removes the
  *text parsing* (names and numbers arrive pre-decoded), but every row still
  passes through the client's encoder. **That encoding is the per-row CPU cost,
  and it is the client bottleneck section 2 isolates.**
- **Flush per TSBS batch, not per row.** Auto-flush is off; the loader flushes on
  each batch boundary (default 10,000 rows). Flushing per batch keeps each
  published frame under the server's ~2 MiB batch cap and amortises the
  round-trip.
- **Publish, not commit, by default.** `Flush` hands the batch to the cursor
  engine and returns; it does not wait for the server. `--qwp-await-ack` instead
  blocks on the server acknowledgement per batch, and a clean `Close` drains and
  waits for all outstanding acks at end of run. This is the "sent vs visible"
  distinction that recurs throughout: a flushed row is on its way, not yet
  queryable.

The pre-encode replay path in section 2 bypasses this entirely: it runs the
client encoder once to produce the WebSocket frames, writes them to disk, then
replays the raw frames. That is why it removes the client cost from the timed
interval and exposes the network and server ceilings underneath.

ILP, by contrast, does almost no client work: the text is already the wire
format, so the loader writes bytes to a socket (TCP) or POSTs them (HTTP). That
asymmetry is the whole reason the same-host comparison is unfair to QWP, and why
the split-host and replay results below matter.

## 1. Split client and server: the result that matters

A real deployment does not run the loader on the database machine. Put the
client on its own r8a.8xlarge in the same subnet and point it at the server over
the private network. Both boxes are 32 vCPU; the link is what the instance class
provides, which these runs show to be about 14.7 Gbit/s.

Rows sent, three rounds at each scale:

| scale | ILP TCP | ILP HTTP | QWP | QWP + ack |
|---|---|---|---|---|
| 1,000 | 5.20-5.22M | 5.10-5.19M | 8.67-9.68M | **11.50-11.61M** |
| 4,000 | 5.29-5.32M | 5.30M | 9.15-9.81M | **11.32-11.95M** |
| 100,000 | 5.26-5.30M | 5.22-5.28M | 8.75-9.07M | 8.99-9.05M |

The wire throughput each of those implies is the whole story:

| transport | rows sent | wire used | of 14.7 Gbit/s |
|---|---|---|---|
| ILP TCP | 5.3M | 14.7 Gbit/s | **saturated** |
| ILP HTTP | 5.3M | 14.7 Gbit/s | **saturated** |
| QWP | 9.4M | 7.6 Gbit/s | 52% |
| QWP + ack | 11.9M | 9.8 Gbit/s | 67% |

Both ILP transports pin at the network ceiling at every cardinality, within 1%
of each other across nine runs each. Line protocol text is ~347 bytes/row, so
5.3M rows/s is simply what the link carries; the protocol, the server and the
client are all idle behind it. QWP's binary format is ~103 bytes/row, so the
same link would carry ~17.8M rows/s, and QWP is not yet close to it: at 7.6
Gbit/s the constraint is the Go client's encoding, not the wire or the server.

So in a split deployment QWP sends **1.7x** ILP plain and **2.2x** with acks,
and has headroom the ILP transports do not. This is where a binary columnar
protocol is supposed to win, and it does. It is invisible in the same-host
sections below because loopback has no bandwidth limit, which hands line
protocol a 3.4x subsidy no real network gives it.

Two caveats worth stating with the number:

- At 100,000 hosts, plain QWP commits (rows made visible) at only ~1.1M rows/s
  while sending 9M. With `--qwp-await-ack` it holds ~6.8M, the best of any
  transport at that scale. At high cardinality acks are required, not optional.
- 9.4M is what the Go client can push, not what the wire or server can take.
  Removing the client encoding (section 2) lifts QWP to 18.5M over this same
  network, still with every row verified visible. The 9.4M is a client-encoding
  limit, and closing it is the single biggest lever for QWP ingestion.

## 2. Where the bottleneck is: client, network, or server

Section 1 shows QWP over the network at 9.4M rows/s using half the link, so
something other than the wire is capping it. To find out what, run the same 69.1M
rows three ways with `--qwp-preencode-replay`, which encodes all the QWP frames
to disk first and times only the replay-and-acknowledge. That takes the Go
client's row-building and encoding out of the timed path, leaving just network
and server. Every run below was confirmed to make all 69,120,000 rows visible,
not merely sent.

| configuration | rows/s sent | bounded by |
|---|---|---|
| QWP via the Go client, over network | 9.4M | **client encoding** |
| QWP pre-encode replay, over network | **18.3-19.3M** | **network** (14.7 Gbit/s) |
| QWP pre-encode replay, localhost | **45-50M** | **server** |

Three separate ceilings, cleanly separated:

- The **Go client** caps QWP at 9.4M. Doubling to 18.5M by pre-encoding is the
  measure of what the client encoding costs, and it is the biggest single lever
  for QWP ingestion. (Raphael's flag; the frame pre-encoding it does is
  deliberately excluded from the timed interval, so this is a server-and-network
  diagnostic, not an end-to-end loader number.)
- The **network** caps the replay at 18.5M: 18.5M x 103 bytes x 8 = 15.2 Gbit/s,
  right at the 14.7 Gbit/s the ILP transports measured as the link ceiling.
- The **server** ingests 45-50M on localhost, which physically cannot cross the
  wire (that would need 40 Gbit/s), so it only appears on loopback. This is the
  QWP server's true ingest capacity, and it is ~4-9x what ILP reaches.

Two notes. The replay path crashes above ~20,000 rows per frame
(`batch too large ... sendfile: broken pipe`); 10k and 20k are clean, so those
are the figures above. And "sent" is not "visible": the server applies the
write-ahead log at ~6.5M rows/s at this scale (section 7), so replayed rows land
over ~10s rather than at 18M/s. On rows sent QWP beats ILP by the wire-size
ratio; on rows made queryable both are WAL-apply-bound and much closer.

### The CPU split, measured

Direct evidence for "the client competes with the server for cores", sampled
from `/proc` during a co-located 4,000-host load (100% = one core, box has
3200%):

| load | loader CPU | server CPU | total |
|---|---|---|---|
| QWP | 958% | 2006% | 2964% |
| QWP + ack | 1165% | 1817% | 2982% |
| ILP TCP | 169% | 2557% | 2726% |
| ILP HTTP | 1263% | 1670% | 2933% |

The QWP loader burns ~10 of 32 cores encoding rows; the ILP/TCP loader burns
~1.7, because its text is already the wire format and it only writes bytes to a
socket. On a shared box every core the QWP client takes is one the server does
not get, which is why co-located QWP and ILP come out level despite QWP doing
far less work server-side. Normalised per million rows/s, QWP costs the server
**1.81 cores** against ILP/TCP's 2.11 and ILP/HTTP's 2.39 - QWP is the most
server-efficient transport, and that efficiency only turns into throughput once
the client is off the box (sections 1 and 2). These figures were measured once
and are not in a saved results file; they live only here.

### Direct-frame rate across cardinalities

The pre-encode control figure (rows sent, loader's own timer, batch 10k) was not
run at every scale. What exists:

| scale | over network | localhost | rows verified visible |
|---|---|---|---|
| 1,000 | not run | not run | - |
| 4,000 | 18.3-19.3M | 45-50M | yes |
| 100,000 | 12.1-12.3M | not run | send only |

The 4,000-host row is the complete one, measured both ways with every row
confirmed visible. The 100,000-host network figure is trustworthy as a send rate
(it comes from the loader's own timer, which excludes pre-encoding) but its
committed count was not verified. There is no 1,000-host direct-frame run.

The finding worth keeping: even with the client removed, the server's pure-frame
ingest rate **falls with cardinality**, 18M at 4,000 hosts to 12M at 100,000.
This is the clearest signal in the whole set of the server-side cost of high
cardinality, and it is the pre-encode number, so it is not a client artefact.
For comparison the Go-client QWP send rate over the network barely moves with
cardinality (9.4M at 4k, 9.0M at 100k) because the client, not the server, is
its limit at both.

## 3. Same host, stock defaults, 4,000 hosts

69.1M rows, nightly build, nothing configured. Rows sent per second, two rounds:

| transport | round 1 | round 2 |
|---|---|---|
| ILP over TCP | 1.68M | 1.66M |
| ILP over HTTP | 7.14M | 6.94M |
| QWP | 10.50M | 11.49M |
| QWP + per-batch ack | 13.57M | 14.49M |

Read on its own this says QWP is 6-8x ILP/TCP, and that is how it looked this
morning. It is wrong, and the ILP/TCP row is the reason.

## 4. The ILP/TCP number is a thread-pool default

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

## 5. Fair comparison, 4,000 hosts

Same nightly server, ILP pools at 31 so ILP performs as it does on the release.
Three rounds, rows sent:

| transport | r1 | r2 | r3 | mean |
|---|---|---|---|---|
| ILP over TCP | 9.60M | 12.87M | 12.29M | 11.6M |
| QWP | 10.43M | 11.66M | 11.30M | 11.1M |
| QWP + per-batch ack | 13.97M | 13.73M | 13.67M | **13.8M** |

Plain QWP is marginally *behind* ILP/TCP. Only with per-batch acks does it lead,
by 19%. Everything above that came from ILP being hobbled.

## 6. Other scales

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

## 7. Write-ahead log apply workers make no difference

Worth ruling out: the server runs 3 `wal-apply` threads by default on a 32-core
box. Sweeping `QDB_WAL_APPLY_WORKER_COUNT` to 8, 16 and 31 (verified as taking
effect) changed nothing for any transport at 4,000 hosts, on send rate or
otherwise. TSBS writes a single table and apply appears to parallelise per
table, so the extra workers have nothing to share. Whether the default of 3 is
adequate for a multi-table workload is untested.

## 8. Queries

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

## 9. Data volume

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
