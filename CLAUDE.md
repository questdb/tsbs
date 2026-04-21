# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Repository context

This is QuestDB's fork of [timescale/tsbs](https://github.com/timescale/tsbs) — the Time Series Benchmark Suite. It's a collection of Go programs that generate datasets, load them into time series databases, and run benchmark queries. QuestDB-specific changes over the upstream fork:

- Loader optimizations for QuestDB (claims 4M+ rows/s).
- Query-generation bug fixes.
- InfluxDB v2 support (upstream only has v1).

Benchmarking has three phases, each with its own binary family: **generate** (`tsbs_generate_data`, `tsbs_generate_queries`), **load** (`tsbs_load`, `tsbs_load_<db>`), and **query** (`tsbs_run_queries_<db>`). See `README.md` for the full list of supported databases and use cases (`cpu-only`, `devops`, `iot`).

## Build / test / lint

Go 1.23. Everything is driven by the `Makefile`:

- `make` — build all generators, loaders, and query runners into `./bin/` and install.
- `make tsbs_load_questdb` (or any `tsbs_*` name) — build one binary; the Makefile has a generic `tsbs_%` target.
- `make test` — `go test -v ./...`. CI runs with `-race` (`.github/workflows/go.yml`).
- `make coverage` — race-enabled tests with coverage profile.
- `make fmt` / `make checkfmt` — gofmt; `make lint` — golangci-lint (installed on demand).

Run a single test the normal Go way, e.g. `go test -run TestSerialize ./pkg/targets/questdb/...`.

## Architecture

### Target plugin model

Every supported database is a "target" that plugs into a shared benchmarking runtime via the `targets.ImplementedTarget` interface (`pkg/targets/targets.go`). A target provides:

- `TargetName()` and `TargetSpecificFlags()` — CLI flag contributions.
- `Serializer()` — turns `data.Point` into the on-disk format consumed by its loader.
- `Benchmark()` — returns a `targets.Benchmark` that wires up `DataSource`, `BatchFactory`, `PointIndexer`, `Processor`, and `DBCreator`.

Targets are dispatched by format string in `pkg/targets/initializers/target_initializers.go`. Each target lives under `pkg/targets/<db>/` (implementation + serializer) and `cmd/tsbs_load_<db>/` (per-DB main + processor/creator/scanner).

### Load pipeline

`load/loader.go` (`CommonBenchmarkRunner` / `GetBenchmarkRunner`) owns the generic insert loop: it reads from a `DataSource`, partitions points via a `PointIndexer`, hands batches to per-worker `Processor`s, and reports throughput every `--reporting-period`. Two scan modes exist: `scan_with_flow_control.go` (default, applies back-pressure) and `scan_no_flow_control.go` (used when `NoFlowControl` is set — e.g. QuestDB ILP). `load/insertstrategy/` controls `--insert-intervals` throttling.

### QuestDB target specifics

`cmd/tsbs_load_questdb/main.go` does NOT call `BenchmarkRunnerConfig.AddToFlagSet` — it registers a curated subset of flags manually and forces `HashWorkers=false` and `NoFlowControl=true`. It adds QuestDB-only flags: `--ilp-bind-to`, `--url`, `--tls`, `--auth-id`, `--auth-token`. The processor (`process.go`) opens a raw TCP (or TLS) connection and speaks InfluxDB Line Protocol directly. Query runner lives in `cmd/tsbs_run_queries_questdb/` and hits QuestDB's REST endpoint via `http_client.go`.

### Data generation

`cmd/tsbs_generate_data/` and `cmd/tsbs_generate_queries/` are thin mains over `internal/inputs/` (`DataGenerator`, `QueryGenerator`). Use cases are in `pkg/data/usecases/{devops,iot,common}` — each defines simulators and measurements. Generated points flow through the target's `Serializer` so each DB gets its native on-disk format.

### Query runtime

`pkg/query/` holds the generic query benchmarker (`benchmarker.go`, `scanner.go`, `stat_processor.go`) plus one query-type struct per database (`timescaledb.go`, `questdb.go` via REST, `mongo.go`, etc.). Query factories register use-case → query-type implementations in `pkg/query/factories/init_factories.go`.

### Unified `tsbs_load`

`cmd/tsbs_load/` is a Cobra-based front-end that can target any DB via a YAML config (`tsbs_load config --target=<db>` then `tsbs_load load <db> --config=...`). It sits on top of the same per-target plumbing as the standalone `tsbs_load_<db>` binaries. See `docs/tsbs_load.md`.

## Conventions worth noting

- The module path is `github.com/questdb/tsbs` even though this is a QuestDB fork of a timescale-named upstream — don't "fix" imports to `timescale/tsbs`.
- The viper import is `github.com/blagojts/viper` (a fork), not upstream spf13 viper — preserve this when touching flag/config code.
- Helper shell scripts under `scripts/load/` and `scripts/run_queries/` wrap the binaries with sensible defaults; `scripts/generate_run_script.py` emits multi-query run scripts. These are contracts users rely on — preserve their env-var surface (`NUM_WORKERS`, `BATCH_SIZE`, `BULK_DATA_DIR`, `DATABASE_NAME`, …) when editing.
