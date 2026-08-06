#!/usr/bin/env python3
"""Is write-ahead log apply the ceiling every transport is hitting?

Every transport commits at roughly 5-7M rows/s regardless of how fast it
sends, and the server runs 3 wal-apply threads by default on a 32-core
box. This runs the same loads against 3, 8, 16 and 31 apply workers.

Called once per wal worker count; the container is restarted between them
by the shell wrapper. Results append to one file.
"""

import json
import os
import subprocess
import sys
import time
import urllib.parse
import urllib.request

BIN = "/home/ubuntu/tsbs/bin"
LOAD = BIN + "/tsbs_load_questdb"
GEN = BIN + "/tsbs_generate_data"
DATA = "/home/ubuntu/data"
BASE = "http://127.0.0.1:9000/exec"
OUT = "/home/ubuntu/walapply.json"
EXPECTED = 69120000
WORKERS = 32
ROUNDS = 2

TEXT = DATA + "/wal4000.txt"
BINARY = DATA + "/wal4000.qwp"


def exec_sql(q, timeout=300):
    url = BASE + "?" + urllib.parse.urlencode({"query": q})
    with urllib.request.urlopen(url, timeout=timeout) as r:
        return json.load(r)


def count_rows():
    try:
        return exec_sql("select count from cpu")["dataset"][0][0]
    except Exception:
        return 0


def wait_committed(start, label):
    last, last_change = -1, time.time()
    while True:
        n = count_rows()
        if n >= EXPECTED:
            return time.time() - start, n
        if n != last:
            last, last_change = n, time.time()
        elif time.time() - last_change > 240:
            print("    %s stalled at %d" % (label, n), flush=True)
            return time.time() - start, n
        time.sleep(0.25)


def main():
    wal_workers = sys.argv[1]

    if not os.path.exists(TEXT):
        common = ["--use-case=cpu-only", "--seed=123", "--scale=4000",
                  "--timestamp-start=2016-01-01T00:00:00Z",
                  "--timestamp-end=2016-01-03T00:00:00Z", "--log-interval=10s"]
        a = subprocess.Popen([GEN] + common + ["--format=questdb", "--file=" + TEXT])
        b = subprocess.Popen([GEN] + common + ["--format=questdb-qwp", "--file=" + BINARY])
        a.wait()
        b.wait()
        print("data generated", flush=True)

    results = []
    if os.path.exists(OUT):
        with open(OUT) as fh:
            results = json.load(fh)

    configs = [
        ("ilp tcp", TEXT, ["--protocol=ilp"]),
        ("qwp", BINARY, []),
        ("qwp ack", BINARY, ["--qwp-await-ack"]),
    ]
    for rnd in range(1, ROUNDS + 1):
        for label, data, extra in configs:
            exec_sql("drop table if exists cpu")
            time.sleep(3)
            start = time.time()
            p = subprocess.run([LOAD, "--file=" + data, "--workers=" + str(WORKERS)] + extra,
                               capture_output=True, text=True)
            loader_secs = time.time() - start
            if p.returncode != 0:
                tail = [l for l in (p.stdout + p.stderr).strip().splitlines() if l.strip()][-2:]
                print("wal=%-3s %-9s FAILED %s" % (wal_workers, label, " | ".join(tail)), flush=True)
                continue
            commit_secs, n = wait_committed(start, label)
            print("wal=%-3s %-9s r%d  send %9.0f rows/s | committed %9.0f rows/s"
                  % (wal_workers, label, rnd,
                     EXPECTED / loader_secs, n / commit_secs), flush=True)
            results.append({
                "wal_workers": wal_workers, "label": label, "round": rnd,
                "loader_rows_s": EXPECTED / loader_secs,
                "committed_rows_s": n / commit_secs, "rows": n,
            })
            with open(OUT, "w") as fh:
                json.dump(results, fh, indent=2)


main()
