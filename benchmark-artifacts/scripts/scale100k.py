#!/usr/bin/env python3
"""Reproduce the published 100K-host figure.

Two unknowns are tested at once:

  build   - the released image versus the nightly, since ILP/TCP is served
            from different thread pools on each
  window  - 100,000 hosts reach 86.4M rows either as 864 intervals of 10s
            (a 2h24m window) or as 864 intervals of 200s (a 2-day window).
            Same row count, different data shape.

Run under one build at a time; pass the build label as argv[1].
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
EXPECTED = 86400000
WORKERS = 32
ROUNDS = 2

# label, end timestamp, log interval
WINDOWS = [
    ("dense-2h24m", "2016-01-01T02:24:00Z", "10s"),
    ("sparse-2day", "2016-01-03T00:00:00Z", "200s"),
]


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
        elif time.time() - last_change > 180:
            print("    %s stalled at %d" % (label, n), flush=True)
            return time.time() - start, n
        time.sleep(0.25)


def main():
    build = sys.argv[1]
    out = "/home/ubuntu/scale100k-%s.json" % build
    results = []

    for wlabel, end, interval in WINDOWS:
        text = "%s/100k-%s.txt" % (DATA, wlabel)
        t0 = time.time()
        subprocess.run([GEN, "--use-case=cpu-only", "--seed=123",
                        "--scale=100000",
                        "--timestamp-start=2016-01-01T00:00:00Z",
                        "--timestamp-end=" + end,
                        "--log-interval=" + interval,
                        "--format=questdb", "--file=" + text], check=True)
        print("%s / %s: generated in %.0fs, %.1f GB"
              % (build, wlabel, time.time() - t0, os.path.getsize(text) / 1e9),
              flush=True)

        for rnd in range(1, ROUNDS + 1):
            for label, extra in [("ilp tcp", ["--protocol=ilp"])]:
                exec_sql("drop table if exists cpu")
                time.sleep(3)
                start = time.time()
                p = subprocess.run(
                    [LOAD, "--file=" + text, "--workers=" + str(WORKERS)] + extra,
                    capture_output=True, text=True)
                loader_secs = time.time() - start
                if p.returncode != 0:
                    tail = [l for l in (p.stdout + p.stderr).strip().splitlines() if l.strip()][-2:]
                    print("%s %-12s %-9s FAILED %s" % (build, wlabel, label, " | ".join(tail)), flush=True)
                    continue
                commit_secs, n = wait_committed(start, label)
                print("%-8s %-12s %-9s r%d  send %9.0f rows/s | committed %9.0f rows/s"
                      % (build, wlabel, label, rnd,
                         EXPECTED / loader_secs, n / commit_secs), flush=True)
                results.append({
                    "build": build, "window": wlabel, "label": label, "round": rnd,
                    "loader_rows_s": EXPECTED / loader_secs,
                    "committed_rows_s": n / commit_secs, "rows": n,
                })
                with open(out, "w") as fh:
                    json.dump(results, fh, indent=2)
        os.remove(text)
    print("done, saved " + out, flush=True)


main()
