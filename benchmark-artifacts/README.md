# Raw benchmark artifacts

Raw output and harness from the QWP vs ILP runs written up in
[../docs/questdb-qwp-benchmark.md](../docs/questdb-qwp-benchmark.md).

Hardware: AWS EC2 r8a.8xlarge (32 vCPU AMD EPYC 9R45, 247 GB RAM), gp3 500 GB
at 20,000 IOPS / 1000 MB/s, eu-west-1b, Ubuntu 22.04. Client and server on the
same box. Data: cpu-only, seed 123, scale 4000, 2016-01-01 to 2016-01-03, 10s
interval, 69,120,000 rows. 32 workers throughout.

## Results

Every run records both the loader's own timer (`loader_rows_s`, the send rate)
and wall time until the server's row count reaches 69,120,000
(`committed_rows_s`).

| file | what it holds |
|---|---|
| `ingest-results.json` | first A/B, nightly with default ILP pools: ILP/TCP vs QWP from text and binary |
| `ingest-results-3way.json` | adds ILP/HTTP, still on nightly defaults, so ILP/TCP is the 2-thread case |
| `ingest-final.json` | the headline run: nightly with `QDB_LINE_TCP_IO_WORKER_COUNT=16`, three rounds of ILP/TCP, ILP/HTTP, QWP and QWP with per-batch ack |
| `ingest-results-release.json` | QuestDB 9.4.3 release image, defaults, both ILP transports |
| `ack-debug.json` | QWP with and without per-batch ack, split into publish / drain / WAL-apply phases |
| `query-results.json` | 16 cpu-only query types over pg, http and qwp, 1000 queries each, plus the row-count cross-check |
| `scale-sweep.json` | 100, 1,000 and 100,000 hosts, all four ingestion configurations |
| `scale100k-release.json`, `scale100k-nightly-stock.json`, `scale100k-nightly-tuned.json` | 100K hosts on each build and ILP pool size, both window interpretations |
| `samebuild.json` | the fair fight: ILP/TCP with 31 io workers versus QWP on one nightly server, 4,000 and 100,000 hosts |
| `walapply.json` | `QDB_WAL_APPLY_WORKER_COUNT` at 3, 8, 16 and 31 |
| `split-results.json` | first split-host run, 4,000 hosts only |
| `split-results-17280000.json`, `-69120000.json`, `-86400000.json` | split-host sweep at 1,000 / 4,000 / 100,000 hosts, loader on a second instance over the private network. These carry the `wire_gbit_s` field, which is where the ILP transports are shown pinned at 14.7 Gbit/s |
| `split-replay-69120000.json`, `split-replay-86400000.json` | first `--qwp-preencode-replay` attempt over the network. Superseded: the harness mis-timed the committed count (wall clock included the ~50s pre-encode phase). Kept for provenance; use the two files below instead |
| `replay-localhost-69120000.json` | corrected pre-encode replay on the server box over loopback: 45-50M rows/s, all rows verified visible. QWP's server-side ingest ceiling |
| `replay-network-69120000.json` | corrected pre-encode replay from the client box over the network: 18.3-19.3M rows/s, all rows verified visible. Network-bound, matching the 14.7 Gbit/s link. Both files: batch 30k+ crashes the replay frame cap, so only 10k and 20k rounds are present |

The split-host runs used a second r8a.8xlarge (`i-0296bb57cfdd46c1f`) as the
client, talking to the server at its private IP over the same subnet. That is
the topology in section 1 of the write-up, and the one where QWP's advantage is
real rather than a rounding error.

## Harness

`scripts/` holds everything that produced the above, in the order it ran:

| script | role |
|---|---|
| `setup.sh` | Docker, Go, TSBS build, QuestDB container |
| `gen.sh` | QWP smoke test, then generates both data formats |
| `bench.py` | first ingestion A/B |
| `hostnet.sh` | reran it on host networking, ruling out docker-proxy |
| `diag.sh`, `verify_conns.sh`, `ilpio.sh` | why ILP/TCP was slow: CPU sampling, connection counts, thread pools, and the 16-worker retest |
| `bench3.py`, `rerun3.sh`, `final.sh` | three-transport matrix and the release-image comparison |
| `bench4.py`, `run4.sh` | the final four-way run, each transport with its native format |
| `ackdebug.py` | the ack phase breakdown |
| `bench_ilp.py`, `release_ilp.sh` | both ILP transports on the released build |
| `qbench.py` | the query suite |
| `status.sh`, `status2.sh`, `estimate.sh`, `fetchlog.sh` | progress checks during long runs |

Two things to carry forward if these are rerun. `high-cpu-all` dominates the
query suite, roughly 16-25 minutes per transport against 69M rows where every
other type takes seconds. And run the harness under `nohup` with unbuffered
output and incremental writes: a dropped SSH connection killed the query driver
here, and only the final JSON dump saved the results.
