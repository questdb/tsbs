#!/usr/bin/env python3
"""Query benchmark on the box: all 16 cpu-only query types over pg, http and qwp.

Generates the query files once, then runs each type on each transport with a
single worker, as the skill prescribes. Also cross-checks the rows returned
by qwp against the http response, so a transport that silently returns
nothing cannot look fast.
"""

import json
import os
import re
import subprocess
import sys

BIN = "/home/ubuntu/tsbs/bin"
GEN = BIN + "/tsbs_generate_queries"
RUN = BIN + "/tsbs_run_queries_questdb"
QDIR = "/home/ubuntu/queries"

QTYPES = [
    "cpu-max-all-1", "cpu-max-all-8", "cpu-max-all-32-24",
    "single-groupby-1-1-1", "single-groupby-1-1-12", "single-groupby-1-8-1",
    "single-groupby-5-1-1", "single-groupby-5-1-12", "single-groupby-5-8-1",
    "double-groupby-1", "double-groupby-5", "double-groupby-all",
    "high-cpu-1", "high-cpu-all",
    "lastpoint", "groupby-orderby-limit",
]

COMMON = [
    "--use-case=cpu-only", "--seed=123", "--scale=4000",
    "--timestamp-start=2016-01-01T00:00:00Z",
    "--timestamp-end=2016-01-03T00:00:01Z",
    "--queries=1000", "--format=questdb",
]


def gen():
    os.makedirs(QDIR, exist_ok=True)
    for q in QTYPES:
        out = "%s/%s.txt" % (QDIR, q)
        if os.path.exists(out) and os.path.getsize(out) > 0:
            continue
        with open(out, "wb") as fh:
            p = subprocess.run([GEN] + COMMON + ["--query-type=" + q],
                               stdout=fh, stderr=subprocess.PIPE)
        if p.returncode != 0:
            raise SystemExit("generate failed for %s: %s" % (q, p.stderr.decode()))
    print("query files ready")


def run(qtype, protocol, nqueries, debug=False):
    args = [RUN, "--file=%s/%s.txt" % (QDIR, qtype), "--workers=1",
            "--print-interval=0", "--max-queries=" + str(nqueries),
            "--query-protocol=" + protocol]
    if debug:
        args.append("--debug=4")
    if protocol == "http":
        args.append("--url=http://127.0.0.1:9000/")
    elif protocol == "pg":
        args += ["--pg-host=127.0.0.1", "--pg-port=8812"]
    else:
        args.append("--qwp-addr=127.0.0.1:9000")
    p = subprocess.run(args, capture_output=True, text=True)
    out = p.stdout + p.stderr
    if p.returncode != 0:
        lines = [l for l in out.splitlines() if l.strip()][:2]
        return None, None, out, " | ".join(lines)
    rate = re.search(r"Overall query rate ([0-9.]+) queries/sec", out)
    mean = re.findall(r"mean:\s*([0-9.]+)ms", out)
    return (float(rate.group(1)) if rate else None,
            float(mean[-1]) if mean else None, out, None)


def main():
    nqueries = int(sys.argv[1]) if len(sys.argv) > 1 else 1000
    gen()
    results = {}
    print("%-24s %10s %10s %10s  %10s %10s %10s" %
          ("query", "pg q/s", "http q/s", "qwp q/s",
           "pg mean", "http mean", "qwp mean"))
    for qtype in QTYPES:
        rates, means, errs = [], [], []
        for proto in ("pg", "http", "qwp"):
            rate, mean, _, err = run(qtype, proto, nqueries)
            results.setdefault(qtype, {})[proto] = {
                "queries_per_sec": rate, "mean_ms": mean, "error": err,
            }
            rates.append("ERR" if err else ("%.1f" % rate if rate else "?"))
            means.append("ERR" if err else ("%.2f" % mean if mean else "?"))
            if err:
                errs.append("%s: %s" % (proto, err))
        print("%-24s %10s %10s %10s  %10s %10s %10s" %
              tuple([qtype] + rates + means))
        for e in errs:
            print("    " + e)

    print("\nrow-count cross-check, one query per type")
    print("%-24s %12s %12s %8s" % ("query", "http rows", "qwp rows", "match"))
    for qtype in QTYPES:
        _, _, qwp_out, err = run(qtype, "qwp", 1, debug=True)
        m = re.search(r"rows: (\d+)", qwp_out) if not err else None
        qwp_rows = int(m.group(1)) if m else None
        _, _, http_out, err2 = run(qtype, "http", 1, debug=True)
        m = re.search(r'"count":(\d+)', http_out) if not err2 else None
        http_rows = int(m.group(1)) if m else None
        ok = "yes" if (qwp_rows is not None and qwp_rows == http_rows) else "NO"
        results[qtype]["rows"] = {"http": http_rows, "qwp": qwp_rows, "match": ok}
        print("%-24s %12s %12s %8s" % (qtype, http_rows, qwp_rows, ok))

    with open("/home/ubuntu/query-results.json", "w") as fh:
        json.dump(results, fh, indent=2)
    print("saved /home/ubuntu/query-results.json")


main()
