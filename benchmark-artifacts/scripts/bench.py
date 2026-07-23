#!/usr/bin/env python3
"""TSBS ingestion A/B on the benchmark box: ILP text vs QWP binary.

Runs on the instance. For each protocol it drops the table, loads, records
the loader's own summary, then polls the server until the committed row
count stops rising, so both protocols are also compared on rows the server
actually confirmed.
"""

import json
import re
import subprocess
import sys
import time
import urllib.parse
import urllib.request

BIN = "/home/ubuntu/tsbs/bin/tsbs_load_questdb"
TEXT = "/home/ubuntu/data/questdb-data.txt"
BINARY = "/home/ubuntu/data/questdb-data.qwp"
BASE = "http://127.0.0.1:9000/exec"
EXPECTED = 69120000  # 4000 hosts, 2 days, 10s interval


def exec_sql(q, timeout=120):
    url = BASE + "?" + urllib.parse.urlencode({"query": q})
    with urllib.request.urlopen(url, timeout=timeout) as r:
        return json.load(r)


def count_rows():
    try:
        return exec_sql("select count from cpu")["dataset"][0][0]
    except Exception:
        return 0


def wait_committed(expected, start, label):
    """Poll until the count reaches expected, or stops moving for 60s."""
    last, last_change = -1, time.time()
    while True:
        n = count_rows()
        if n >= expected:
            return time.time() - start, n
        if n != last:
            last, last_change = n, time.time()
        elif time.time() - last_change > 60:
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
    out = p.stdout + p.stderr
    if p.returncode != 0:
        print("%-22s FAILED: %s" % (label, out.strip().splitlines()[-2:]))
        return None
    m = re.search(r"loaded (\d+) rows in ([0-9.]+)sec .* mean rate ([0-9.]+) rows/sec", out)
    reported = float(m.group(3)) if m else 0.0
    commit_secs, n = wait_committed(EXPECTED, start, label)
    res = {
        "label": label, "workers": workers,
        "loader_secs": loader_secs, "reported_rows_s": reported,
        "commit_secs": commit_secs, "committed_rows_s": n / commit_secs,
        "rows": n,
    }
    print("%-22s w=%-3d loader %7.2fs (%9.0f rows/s reported) | committed %7.2fs (%9.0f rows/s) | rows %d"
          % (label, workers, loader_secs, reported, commit_secs,
             n / commit_secs, n))
    return res


def main():
    workers = int(sys.argv[1]) if len(sys.argv) > 1 else 32
    rounds = int(sys.argv[2]) if len(sys.argv) > 2 else 2
    results = []
    for r in range(rounds):
        print("--- round %d, %d workers ---" % (r + 1, workers))
        results.append(run("ilp text", TEXT, ["--protocol=ilp"], workers))
        results.append(run("qwp from text", TEXT, [], workers))
        results.append(run("qwp from binary", BINARY, [], workers))
        results.append(run("qwp binary, ack", BINARY, ["--qwp-await-ack"], workers))
    with open("/home/ubuntu/ingest-results.json", "w") as fh:
        json.dump([r for r in results if r], fh, indent=2)
    print("saved /home/ubuntu/ingest-results.json")


main()
