#!/usr/bin/env python3
"""Same-build comparison: ILP/TCP with fully sized pools versus QWP.

Runs both transports against one nightly server at two scales, so the
QWP-versus-ILP numbers no longer come from different builds. The release
build's ILP figures are the target ILP has to reach for this to count as
a fair fight.
"""

import json
import os
import subprocess
import time
import urllib.parse
import urllib.request

BIN = "/home/ubuntu/tsbs/bin"
LOAD = BIN + "/tsbs_load_questdb"
GEN = BIN + "/tsbs_generate_data"
DATA = "/home/ubuntu/data"
BASE = "http://127.0.0.1:9000/exec"
OUT = "/home/ubuntu/samebuild.json"
WORKERS = 32
ROUNDS = 3

# scale, end, interval, expected rows
SCALES = [
    (4000, "2016-01-03T00:00:00Z", "10s", 69120000),
    (100000, "2016-01-01T02:24:00Z", "10s", 86400000),
]

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
    start = time.time()
    p = subprocess.run([LOAD, "--file=" + data, "--workers=" + str(WORKERS)] + extra,
                       capture_output=True, text=True)
    loader_secs = time.time() - start
    if p.returncode != 0:
        tail = [l for l in (p.stdout + p.stderr).strip().splitlines() if l.strip()][-2:]
        print("scale %-7d %-10s FAILED %s" % (scale, label, " | ".join(tail)), flush=True)
        return
    commit_secs, n = wait_committed(expected, start, label)
    print("scale %-7d %-10s r%d  send %9.0f rows/s | committed %9.0f rows/s"
          % (scale, label, rnd, expected / loader_secs, n / commit_secs), flush=True)
    results.append({
        "scale": scale, "label": label, "round": rnd,
        "loader_rows_s": expected / loader_secs,
        "committed_rows_s": n / commit_secs, "rows": n,
    })
    with open(OUT, "w") as fh:
        json.dump(results, fh, indent=2)


def main():
    for scale, end, interval, expected in SCALES:
        text = "%s/sb%d.txt" % (DATA, scale)
        binary = "%s/sb%d.qwp" % (DATA, scale)
        common = ["--use-case=cpu-only", "--seed=123", "--scale=" + str(scale),
                  "--timestamp-start=2016-01-01T00:00:00Z",
                  "--timestamp-end=" + end, "--log-interval=" + interval]
        t0 = time.time()
        a = subprocess.Popen([GEN] + common + ["--format=questdb", "--file=" + text])
        b = subprocess.Popen([GEN] + common + ["--format=questdb-qwp", "--file=" + binary])
        a.wait()
        b.wait()
        print("scale %d generated in %.0fs" % (scale, time.time() - t0), flush=True)

        configs = [
            ("ilp tcp", text, ["--protocol=ilp"]),
            ("qwp", binary, []),
            ("qwp ack", binary, ["--qwp-await-ack"]),
        ]
        for rnd in range(1, ROUNDS + 1):
            for label, data, extra in configs:
                run(scale, label, data, extra, expected, rnd)
        os.remove(text)
        os.remove(binary)
        print("scale %d done" % scale, flush=True)
    print("same-build comparison complete, saved " + OUT, flush=True)


main()
