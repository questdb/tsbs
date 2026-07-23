#!/usr/bin/env bash
# Only ilpio_0 and ilpio_1 appeared during the TCP load. Count the ILP I/O
# threads, then restart QuestDB with more of them and measure ILP/TCP again.
set -euo pipefail
BIN=/home/ubuntu/tsbs/bin
QPID=$(pgrep -f questdb | head -1)

echo "=== server thread pools, current container ==="
ps -L -o comm= -p "$QPID" | sed 's/_[0-9]*$//' | sort | uniq -c | sort -rn | head -20

echo
echo "=== ilp io threads ==="
ps -L -o comm= -p "$QPID" | grep -c ilpio || true

echo
echo "=== restart with 16 ILP io workers and 16 writer workers ==="
sudo docker rm -f questdb >/dev/null 2>&1 || true
sudo docker run -d --name questdb --network host \
  -e QDB_LINE_TCP_IO_WORKER_COUNT=16 \
  -e QDB_LINE_TCP_WRITER_WORKER_COUNT=16 \
  -v /home/ubuntu/qdbroot:/var/lib/questdb \
  questdb/questdb:nightly
curl -s --retry 60 --retry-delay 2 --retry-all-errors -o /dev/null -w "ping:%{http_code}\n" http://127.0.0.1:9000/ping
sleep 5
QPID=$(pgrep -f questdb | head -1)
echo "new questdb pid: $QPID"

echo
echo "=== ILP/TCP with 16 io workers ==="
curl -s -G --data-urlencode "query=drop table if exists cpu" http://127.0.0.1:9000/exec
sleep 3
$BIN/tsbs_load_questdb --file=/home/ubuntu/data/questdb-data.txt --workers=32 --protocol=ilp &
LOAD=$!
sleep 8
echo "--- ilp io threads now ---"
ps -L -o comm= -p "$QPID" | grep -c ilpio || true
top -H -b -n 1 -p "$QPID" | grep -E "ilpio|shared-write" | head -12
wait $LOAD
echo "=== done ==="
