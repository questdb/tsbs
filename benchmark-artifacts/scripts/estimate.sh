#!/usr/bin/env bash
# How big is one high-cpu-all result set, and how far along is the run?
echo "=== rows matched by a typical high-cpu-all query, 12h window ==="
curl -s -G --data-urlencode "query=select count from cpu where usage_user > 90.0 and timestamp >= '2016-01-01T06:00:00Z' and timestamp < '2016-01-01T18:00:00Z'" http://127.0.0.1:9000/exec
echo
echo "=== total rows over 90 in the whole table ==="
curl -s -G --data-urlencode "query=select count from cpu where usage_user > 90.0" http://127.0.0.1:9000/exec
echo
echo "=== elapsed for the current runner ==="
ps -eo etime,args --sort=-etime | grep tsbs_run_queries | grep -v grep
echo
echo "=== first two queries in the file, to confirm shape ==="
head -c 600 /home/ubuntu/queries/high-cpu-all.txt
