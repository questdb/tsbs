# Go parameters
GOCMD=GO111MODULE=on go
GOBUILD=$(GOCMD) build
GOINSTALL=$(GOCMD) install
GOCLEAN=$(GOCMD) clean
GOTEST=$(GOCMD) test
GOGET=$(GOCMD) get
GOMOD=$(GOCMD) mod
GOFMT=$(GOCMD) fmt

GENERATORS = tsbs_generate_data \
			 tsbs_generate_data_questdb_qwp \
			 tsbs_generate_queries

LOADERS = tsbs_load \
		  tsbs_load_akumuli \
		  tsbs_load_cassandra \
		  tsbs_load_clickhouse \
		  tsbs_load_cratedb \
		  tsbs_load_influx \
		  tsbs_load_mongo \
		  tsbs_load_prometheus \
		  tsbs_load_siridb \
		  tsbs_load_timescaledb \
		  tsbs_load_victoriametrics \
		  tsbs_load_questdb \
		  tsbs_load_questdb_qwp

RUNNERS = tsbs_run_queries_akumuli \
		  tsbs_run_queries_cassandra \
		  tsbs_run_queries_clickhouse \
		  tsbs_run_queries_cratedb \
		  tsbs_run_queries_influx \
		  tsbs_run_queries_mongo \
		  tsbs_run_queries_siridb \
		  tsbs_run_queries_timescaledb \
		  tsbs_run_queries_timestream \
		  tsbs_run_queries_victoriametrics \
		  tsbs_run_queries_questdb

.PHONY: all generators loaders runners lint fmt checkfmt test coverage FORCE

all: generators loaders runners

generators: $(GENERATORS)

loaders: $(LOADERS)

runners: $(RUNNERS)

test:
	$(GOTEST) -v ./...

coverage:
	$(GOTEST) -race -coverprofile=coverage.txt -covermode=atomic ./...

# The tsbs_% targets write to bin/$@, not the current directory — so Make
# can't see the real output and was treating any stray same-named file in
# the repo root as a satisfied target. Depending on FORCE (an empty phony)
# makes every invocation re-run the recipe; Go's build cache takes care of
# actual incremental work.
FORCE:

tsbs_%: FORCE
	$(GOBUILD) -o bin/$@ ./cmd/$@
	$(GOINSTALL) ./cmd/$@

checkfmt:
	@echo 'Checking gofmt';\
 	bash -c "diff -u <(echo -n) <(gofmt -d .)";\
	EXIT_CODE=$$?;\
	if [ "$$EXIT_CODE"  -ne 0 ]; then \
		echo '$@: Go files must be formatted with gofmt'; \
	fi && \
	exit $$EXIT_CODE

lint:
	$(GOGET) github.com/golangci/golangci-lint/cmd/golangci-lint
	golangci-lint run

fmt:
	$(GOFMT) ./...
