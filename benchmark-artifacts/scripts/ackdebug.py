#!/usr/bin/env python3
"""Why is QWP with per-batch ack faster than without?

Splits each run into three phases instead of one wall-clock number:

  publish  - the loader's own summary timer, which stops when the last
             batch has been handed to the sender (Close has not run yet)
  drain    - process wall time minus publish: that is Close, which waits
             for outstanding server acks
  visible  - time after the process exits until the server's row count
             reaches the expected total, i.e. the WAL-apply tail

If the ack variant is "faster" only because its drain moved into publish,
the publish+drain totals will match and this is accounting, not speed.
"""

import json
import re
import subprocess
import sys
import time
import urllib.parse
import urllib.request

BIN = "/home/ubuntu/tsbs/bin/tsbs_load_questdb"
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


def run(label, extra, workers=32):
    exec_sql("drop table if exists cpu")
    time.sleep(3)
    args = [BIN, "--file=" + BINARY, "--workers=" + str(workers)] + extra
    start = time.time()
    p = subprocess.run(args, capture_output=True, text=True)
    wall = time.time() - start
    out = p.stdout + p.stderr
    if p.returncode != 0:
        print("%-12s FAILED %s" % (label, out.strip().splitlines()[-2:]))
        return None
    m = re.search(r"loaded \d+ rows in ([0-9.]+)sec", out)
    publish = float(m.group(1)) if m else float("nan")
    drain = wall - publish
    exit_at = time.time()
    while count_rows() < EXPECTED:
        time.sleep(0.2)
    visible = time.time() - exit_at
    total = time.time() - start
    print("%-12s publish %6.2fs | drain(Close) %6.2fs | wal tail %6.2fs | total %6.2fs "
          "| send %8.0f rows/s | committed %8.0f rows/s"
          % (label, publish, drain, visible, total,
             EXPECTED / publish, EXPECTED / total))
    return {"label": label, "publish": publish, "drain": drain,
            "wal_tail": visible, "total": total}


def main():
    rounds = int(sys.argv[1]) if len(sys.argv) > 1 else 3
    results = []
    for r in range(rounds):
        print("--- round %d ---" % (r + 1))
        results.append(run("no ack", []))
        results.append(run("ack", ["--qwp-await-ack"]))
    with open("/home/ubuntu/ack-debug.json", "w") as fh:
        json.dump([r for r in results if r], fh, indent=2)
    print("saved /home/ubuntu/ack-debug.json")


main()
