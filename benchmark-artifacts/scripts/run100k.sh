#!/usr/bin/env bash
# 100K hosts on both builds, both window interpretations, ILP only.
set -euo pipefail

run_build () {
  local image=$1
  local label=$2
  local root=$3
  shift 3
  sudo docker rm -f questdb >/dev/null 2>&1 || true
  sudo rm -rf "$root"
  mkdir -p "$root"
  sudo docker run -d --name questdb --network host "$@" \
    -v "$root":/var/lib/questdb "$image"
  curl -s --retry 90 --retry-delay 2 --retry-all-errors -o /dev/null -w "ping:%{http_code}\n" http://127.0.0.1:9000/ping
  sleep 5
  curl -s -G --data-urlencode "query=select build" http://127.0.0.1:9000/exec
  echo
  QPID=$(pgrep -f questdb | head -1)
  echo "thread pools:"
  ps -L -o comm= -p "$QPID" | sed 's/_[0-9]*$//' | sort | uniq -c | sort -rn | head -6
  echo
  python3 -u /home/ubuntu/scale100k.py "$label"
}

echo "########## released build, stock config ##########"
run_build questdb/questdb:latest release /home/ubuntu/qdbroot-release

echo
echo "########## nightly, stock config ##########"
run_build questdb/questdb:nightly nightly-stock /home/ubuntu/qdbroot

echo
echo "########## nightly, 16 ILP io workers ##########"
run_build questdb/questdb:nightly nightly-tuned /home/ubuntu/qdbroot \
  -e QDB_LINE_TCP_IO_WORKER_COUNT=16 -e QDB_LINE_TCP_WRITER_WORKER_COUNT=16

echo "=== 100k investigation complete ==="
