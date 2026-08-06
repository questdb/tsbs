#!/usr/bin/env bash
# Measure both ILP transports on the released build, with defaults.
# Nightly numbers already exist; this fills the gap so the docs can say
# "measured on both builds" truthfully.
set -euo pipefail

sudo docker rm -f questdb >/dev/null 2>&1 || true
# The container writes as root, so clear its data dir with sudo.
sudo rm -rf /home/ubuntu/qdbroot-release
mkdir -p /home/ubuntu/qdbroot-release
sudo docker run -d --name questdb --network host \
  -v /home/ubuntu/qdbroot-release:/var/lib/questdb \
  questdb/questdb:latest
curl -s --retry 90 --retry-delay 2 --retry-all-errors -o /dev/null -w "ping:%{http_code}\n" http://127.0.0.1:9000/ping
sleep 5
curl -s -G --data-urlencode "query=select build" http://127.0.0.1:9000/exec
echo
QPID=$(pgrep -f questdb | head -1)
echo "thread pools:"
ps -L -o comm= -p "$QPID" | sed 's/_[0-9]*$//' | sort | uniq -c | sort -rn | head -8
echo
python3 -u /home/ubuntu/bench_ilp.py 32 2
echo "=== release ilp run complete ==="
