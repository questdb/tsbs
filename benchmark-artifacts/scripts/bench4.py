#!/usr/bin/env python3
"""Four-way TSBS ingestion comparison, each transport with its own format.

  ilp tcp   - line protocol text over TCP  (server tuned: 16 ILP io workers)
  ilp http  - line protocol text over HTTP
  qwp       - binary questdb-qwp file over QWP
  qwp ack   - same, awaiting the server ack on every batch
"""

import json
import subprocess
import sys
import time
import urllib.parse
import urllib.request

BIN = "/home/ubuntu/tsbs/bin/tsbs_load_questdb"
TEXT = "/home/ubuntu/data/questdb-data.txt"
BINARY = "/home/ubuntu/data/questdb-data.qwp"
BASE = "http://127.0.0.1:9000/exec"
EXPECTED = 69120000


def exec_sql(q, timeout=180):
    url = BASE + "?" + urllib.parse.urlencode({"query": q})
    with urllib.request.urlopen(url, timeout=timeout) as r:
        return json.load(r)


def count_rows():
    try:
        return exec_sql("select count from cpu")["dataset"][0][0]
    except Exception:
        return 0


def wait_committed(expected, start, label):
    last, last_change = -1, time.time()
    while True:
        n = count_rows()
        if n >= expected:
            return time.time() - start, n
        if n != last:
            last, last_change = n, time.time()
        elif time.time() - last_change > 120:
            print("    %s: count stalled at %d" % (label, n))
            return time.time() - start, n
        time.sleep(0.25)


def run(label, data, extra, workers):
    exec_sql("drop table if exists cpu")
    time.sleep(3)
    args = [BIN, "--file=" + data, "--workers=" + str(workers)] + extra
    start = time.time()
    p = subprocess.run(args, capture_output=True, text=True)
    loader_secs = time.time() - start
    if p.returncode != 0:
        tail = [l for l in (p.stdout + p.stderr).strip().splitlines() if l.strip()][-2:]
        print("%-20s FAILED: %s" % (label, " | ".join(tail)))
        return None
    commit_secs, n = wait_committed(EXPECTED, start, label)
    print("%-20s w=%-3d loader %7.2fs (%9.0f rows/s) | committed %7.2fs (%9.0f rows/s) | rows %d"
          % (label, workers, loader_secs, EXPECTED / loader_secs,
             commit_secs, n / commit_secs, n))
    return {"label": label, "workers": workers,
            "loader_secs": loader_secs, "loader_rows_s": EXPECTED / loader_secs,
            "commit_secs": commit_secs, "committed_rows_s": n / commit_secs,
            "rows": n}


def main():
    workers = int(sys.argv[1]) if len(sys.argv) > 1 else 32
    rounds = int(sys.argv[2]) if len(sys.argv) > 2 else 3
    configs = [
        ("ilp tcp", TEXT, ["--protocol=ilp"]),
        ("ilp http", TEXT, ["--protocol=ilp-http"]),
        ("qwp", BINARY, []),
        ("qwp, ack", BINARY, ["--qwp-await-ack"]),
    ]
    results = []
    for r in range(rounds):
        print("--- round %d, %d workers ---" % (r + 1, workers))
        for label, data, extra in configs:
            res = run(label, data, extra, workers)
            if res:
                res["round"] = r + 1
                results.append(res)
    with open("/home/ubuntu/ingest-final.json", "w") as fh:
        json.dump(results, fh, indent=2)
    print("saved /home/ubuntu/ingest-final.json")


main()
