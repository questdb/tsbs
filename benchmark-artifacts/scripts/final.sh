#!/usr/bin/env bash
# 1. Full matrix against a server with ILP pools sized like the others.
# 2. Then check whether the released image ships the same 2-thread default,
#    which is what Nick suspects came from the thread-pool restructure.
set -euo pipefail
BIN=/home/ubuntu/tsbs/bin

echo "=== full matrix, nightly with 16 ILP io/writer workers ==="
python3 /home/ubuntu/bench3.py 32 2

echo
echo "=== default ilp pool size: nightly vs release ==="
sudo docker rm -f questdb >/dev/null 2>&1 || true
mkdir -p /home/ubuntu/qdbroot-release
sudo docker run -d --name questdb --network host \
  -v /home/ubuntu/qdbroot-release:/var/lib/questdb \
  questdb/questdb:latest
curl -s --retry 90 --retry-delay 2 --retry-all-errors -o /dev/null -w "release ping:%{http_code}\n" http://127.0.0.1:9000/ping
sleep 5
QPID=$(pgrep -f questdb | head -1)
echo "release build:"
curl -s -G --data-urlencode "query=select build" http://127.0.0.1:9000/exec
echo
echo "release thread pools:"
ps -L -o comm= -p "$QPID" | sed 's/_[0-9]*$//' | sort | uniq -c | sort -rn | head -12

echo
echo "=== release image, ILP/TCP with default config ==="
$BIN/tsbs_load_questdb --file=/home/ubuntu/data/questdb-data.txt --workers=32 --protocol=ilp
echo "=== done ==="
