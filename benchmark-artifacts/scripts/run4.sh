#!/usr/bin/env bash
# Restore the nightly (the only build with QWP) with ILP TCP pools sized like
# the rest, so every transport gets a fair server, then run the four-way
# comparison three times.
set -euo pipefail

sudo docker rm -f questdb >/dev/null 2>&1 || true
sudo docker run -d --name questdb --network host \
  -e QDB_LINE_TCP_IO_WORKER_COUNT=16 \
  -e QDB_LINE_TCP_WRITER_WORKER_COUNT=16 \
  -v /home/ubuntu/qdbroot:/var/lib/questdb \
  questdb/questdb:nightly
curl -s --retry 90 --retry-delay 2 --retry-all-errors -o /dev/null -w "ping:%{http_code}\n" http://127.0.0.1:9000/ping
sleep 5
curl -s -G --data-urlencode "query=select build" http://127.0.0.1:9000/exec
echo
QPID=$(pgrep -f questdb | head -1)
echo "thread pools:"
ps -L -o comm= -p "$QPID" | sed 's/_[0-9]*$//' | sort | uniq -c | sort -rn | head -8
echo
python3 /home/ubuntu/bench4.py 32 3
