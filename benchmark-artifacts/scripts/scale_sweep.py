#!/usr/bin/env python3
"""Ingestion across host counts, matching the published TSBS scales.

100 and 1,000 hosts use the same two-day window as the 4,000-host run.
100,000 hosts uses a 2h24m window, which is how the published 86M-row
figure at that scale is reached (100,000 x 864 intervals).

Generates each scale, runs every transport against it, deletes the data
files, then moves on, so peak disk stays at one scale's worth. Results are
written after every single run: a dropped connection cannot lose more than
the run in flight.
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
OUT = "/home/ubuntu/scale-sweep.json"

# scale, end timestamp, expected rows
SCALES = [
    (100, "2016-01-03T00:00:00Z", 1728000),
    (1000, "2016-01-03T00:00:00Z", 17280000),
    (100000, "2016-01-01T02:24:00Z", 86400000),
]

WORKERS = 32
ROUNDS = 2
results = []


def exec_sql(q, timeout=300):
    url = BASE + "?" + urllib.parse.urlencode({"query": q})
    with urllib.request.urlopen(url, timeout=timeout) as r:
        return json.load(r)


def count_rows():
    try:
        return exec_sql("select count from cpu")["dataset"][0][0]
    except Exception:
        return 0


def save():
    with open(OUT, "w") as fh:
        json.dump(results, fh, indent=2)


def generate(scale, end, text, binary):
    common = ["--use-case=cpu-only", "--seed=123", "--scale=" + str(scale),
              "--timestamp-start=2016-01-01T00:00:00Z",
              "--timestamp-end=" + end, "--log-interval=10s"]
    t0 = time.time()
    a = subprocess.Popen([GEN] + common + ["--format=questdb", "--file=" + text])
    b = subprocess.Popen([GEN] + common + ["--format=questdb-qwp", "--file=" + binary])
    a.wait()
    b.wait()
    print("scale %d: generated in %.0fs, text %.1f GB, binary %.1f GB"
          % (scale, time.time() - t0,
             os.path.getsize(text) / 1e9, os.path.getsize(binary) / 1e9),
          flush=True)


def wait_committed(expected, start, label):
    last, last_change = -1, time.time()
    while True:
        n = count_rows()
        if n >= expected:
            return time.time() - start, n
        if n != last:
            last, last_change = n, time.time()
        elif time.time() - last_change > 180:
            print("    %s stalled at %d" % (label, n), flush=True)
            return time.time() - start, n
        time.sleep(0.25)


def run(scale, label, data, extra, expected, rnd):
    exec_sql("drop table if exists cpu")
    time.sleep(3)
    args = [LOAD, "--file=" + data, "--workers=" + str(WORKERS)] + extra
    start = time.time()
    p = subprocess.run(args, capture_output=True, text=True)
    loader_secs = time.time() - start
    if p.returncode != 0:
        tail = [l for l in (p.stdout + p.stderr).strip().splitlines() if l.strip()][-2:]
        print("scale %-7d %-10s FAILED: %s" % (scale, label, " | ".join(tail)), flush=True)
        return
    commit_secs, n = wait_committed(expected, start, label)
    print("scale %-7d %-10s r%d  send %9.0f rows/s | committed %9.0f rows/s | rows %d"
          % (scale, label, rnd, expected / loader_secs, n / commit_secs, n), flush=True)
    results.append({
        "scale": scale, "label": label, "round": rnd, "expected": expected,
        "loader_secs": loader_secs, "loader_rows_s": expected / loader_secs,
        "commit_secs": commit_secs, "committed_rows_s": n / commit_secs,
        "rows": n,
    })
    save()


def main():
    for scale, end, expected in SCALES:
        text = "%s/scale%d.txt" % (DATA, scale)
        binary = "%s/scale%d.qwp" % (DATA, scale)
        generate(scale, end, text, binary)
        configs = [
            ("ilp tcp", text, ["--protocol=ilp"]),
            ("ilp http", text, ["--protocol=ilp-http"]),
            ("qwp", binary, []),
            ("qwp ack", binary, ["--qwp-await-ack"]),
        ]
        for rnd in range(1, ROUNDS + 1):
            for label, data, extra in configs:
                run(scale, label, data, extra, expected, rnd)
        os.remove(text)
        os.remove(binary)
        print("scale %d done, data removed" % scale, flush=True)
    print("sweep complete, saved " + OUT, flush=True)


main()
