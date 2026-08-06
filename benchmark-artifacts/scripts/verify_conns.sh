#!/usr/bin/env bash
# Nick's question: is the ILP/TCP run actually using 32 client workers?
# Count established client connections to 9009 and 9000 during each load,
# and see how many QuestDB threads are actually busy.
set -euo pipefail
BIN=/home/ubuntu/tsbs/bin
QPID=$(pgrep -f questdb | head -1)
echo "questdb pid: $QPID"

sample() {
  local port=$1
  local label=$2
  for i in 1 2 3; do
    local n
    n=$(ss -tan | grep -c ":$port " || true)
    echo "  $label sockets on $port: $n"
    # threads of the server using more than 20% cpu
    local busy
    busy=$(ps -L -o pcpu=,comm= -p "$QPID" | awk '$1 > 20' | wc -l)
    echo "  $label questdb threads over 20% cpu: $busy"
    sleep 3
  done
}

echo "=== ILP over TCP ==="
curl -s -G --data-urlencode "query=drop table if exists cpu" http://127.0.0.1:9000/exec
sleep 3
$BIN/tsbs_load_questdb --file=/home/ubuntu/data/questdb-data.txt --workers=32 --protocol=ilp &
LOAD=$!
sleep 6
sample 9009 "ilp-tcp"
wait $LOAD

echo "=== ILP over HTTP ==="
curl -s -G --data-urlencode "query=drop table if exists cpu" http://127.0.0.1:9000/exec
sleep 3
$BIN/tsbs_load_questdb --file=/home/ubuntu/data/questdb-data.txt --workers=32 --protocol=ilp-http &
LOAD=$!
sleep 3
sample 9000 "ilp-http"
wait $LOAD

echo "=== ILP over TCP, top server threads by cpu ==="
curl -s -G --data-urlencode "query=drop table if exists cpu" http://127.0.0.1:9000/exec
sleep 3
$BIN/tsbs_load_questdb --file=/home/ubuntu/data/questdb-data.txt --workers=32 --protocol=ilp &
LOAD=$!
sleep 10
ps -L -o pcpu=,comm= -p "$QPID" | sort -rn | head -15
wait $LOAD
echo "=== done ==="
