#!/usr/bin/env bash
# Where does the ILP path top out: the loader, or the server?
set -euo pipefail
BIN=/home/ubuntu/tsbs/bin

echo "=== questdb ILP worker config from startup log ==="
sudo docker logs questdb 2>&1 | grep -i -E "line.tcp|ilp|worker" | head -20 || true

echo
echo "=== client-side ceiling: read and parse only, no load ==="
$BIN/tsbs_load_questdb --file=/home/ubuntu/data/questdb-data.txt --workers=32 --do-load=false
$BIN/tsbs_load_questdb --file=/home/ubuntu/data/questdb-data.qwp --workers=32 --do-load=false

echo
echo "=== ILP load with CPU sampling ==="
curl -s -G --data-urlencode "query=drop table if exists cpu" http://127.0.0.1:9000/exec
echo
sleep 3
$BIN/tsbs_load_questdb --file=/home/ubuntu/data/questdb-data.txt --workers=32 --protocol=ilp &
LOADPID=$!
sleep 8
echo "--- top processes during ILP load ---"
top -b -n 3 -d 2 | grep -E "^%Cpu|tsbs_load|java" | head -20
wait $LOADPID

echo
echo "=== same sampling during QWP load ==="
curl -s -G --data-urlencode "query=drop table if exists cpu" http://127.0.0.1:9000/exec
echo
sleep 3
$BIN/tsbs_load_questdb --file=/home/ubuntu/data/questdb-data.qwp --workers=32 &
LOADPID=$!
sleep 3
echo "--- top processes during QWP load ---"
top -b -n 2 -d 1 | grep -E "^%Cpu|tsbs_load|java" | head -20
wait $LOADPID
echo "=== diagnostics complete ==="
