#!/usr/bin/env bash
# Sweep the write-ahead log apply worker count. ILP pools stay at 31 so ILP
# is not the thing being measured; only wal-apply changes.
set -euo pipefail

rm -f /home/ubuntu/walapply.json

for WAL in 3 8 16 31; do
  echo "########## wal-apply workers: $WAL ##########"
  sudo docker rm -f questdb >/dev/null 2>&1 || true
  sudo rm -rf /home/ubuntu/qdbroot
  mkdir -p /home/ubuntu/qdbroot
  sudo docker run -d --name questdb --network host \
    -e QDB_LINE_TCP_IO_WORKER_COUNT=31 \
    -e QDB_LINE_TCP_WRITER_WORKER_COUNT=31 \
    -e QDB_WAL_APPLY_WORKER_COUNT=$WAL \
    -v /home/ubuntu/qdbroot:/var/lib/questdb \
    questdb/questdb:nightly
  curl -s --retry 90 --retry-delay 2 --retry-all-errors -o /dev/null -w "ping:%{http_code}\n" http://127.0.0.1:9000/ping
  sleep 5
  QPID=$(pgrep -f questdb | head -1)
  echo "wal-apply threads actually running:"
  ps -L -o comm= -p "$QPID" | grep -c wal-apply || true
  python3 -u /home/ubuntu/walapply.py "$WAL"
  echo
done
echo "=== wal apply sweep complete ==="
