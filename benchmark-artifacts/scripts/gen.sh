#!/usr/bin/env bash
# QWP smoke test, then generate both data formats at the blog's scale.
set -euo pipefail
BIN=/home/ubuntu/tsbs/bin

echo "=== step 3: QWP smoke test ==="
$BIN/tsbs_generate_data --use-case=cpu-only --seed=123 --scale=1 \
  --timestamp-start="2016-01-01T00:00:00Z" --timestamp-end="2016-01-01T00:01:40Z" \
  --log-interval=10s --format=questdb-qwp --file=/tmp/qwp-smoke.qwp
$BIN/tsbs_load_questdb --file=/tmp/qwp-smoke.qwp --workers=1
echo -n "smoke rows: "
curl -s -G --data-urlencode "query=select count from cpu" http://127.0.0.1:9000/exec
echo
curl -s -G --data-urlencode "query=drop table if exists cpu" http://127.0.0.1:9000/exec
echo
rm -f /tmp/qwp-smoke.qwp

mkdir -p /home/ubuntu/data
echo "=== step 4: generate data, scale 4000, 2 days ==="
COMMON="--use-case=cpu-only --seed=123 --scale=4000 --timestamp-start=2016-01-01T00:00:00Z --timestamp-end=2016-01-03T00:00:00Z --log-interval=10s"

date +%T
$BIN/tsbs_generate_data $COMMON --format=questdb --file=/home/ubuntu/data/questdb-data.txt &
PID_TXT=$!
$BIN/tsbs_generate_data $COMMON --format=questdb-qwp --file=/home/ubuntu/data/questdb-data.qwp &
PID_QWP=$!
wait $PID_TXT
wait $PID_QWP
date +%T

ls -l /home/ubuntu/data/questdb-data.txt /home/ubuntu/data/questdb-data.qwp
df -h /home/ubuntu
echo "=== generation complete ==="
