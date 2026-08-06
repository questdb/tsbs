#!/usr/bin/env bash
# Scale sweep: back to the nightly with ILP pools sized like the shared ones,
# so these numbers sit alongside the 4,000-host run.
set -euo pipefail

sudo docker rm -f questdb >/dev/null 2>&1 || true
sudo rm -rf /home/ubuntu/qdbroot
mkdir -p /home/ubuntu/qdbroot
sudo docker run -d --name questdb --network host \
  -e QDB_LINE_TCP_IO_WORKER_COUNT=16 \
  -e QDB_LINE_TCP_WRITER_WORKER_COUNT=16 \
  -v /home/ubuntu/qdbroot:/var/lib/questdb \
  questdb/questdb:nightly
curl -s --retry 90 --retry-delay 2 --retry-all-errors -o /dev/null -w "ping:%{http_code}\n" http://127.0.0.1:9000/ping
sleep 5
curl -s -G --data-urlencode "query=select build" http://127.0.0.1:9000/exec
echo
echo "disk before:"
df -h /home/ubuntu
echo
rm -f /home/ubuntu/data/questdb-data.txt /home/ubuntu/data/questdb-data.qwp
python3 -u /home/ubuntu/scale_sweep.py
echo "disk after:"
df -h /home/ubuntu
