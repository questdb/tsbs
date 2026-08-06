#!/usr/bin/env bash
# Can the nightly's ILP/TCP match the release once its pools are sized like
# the shared ones? 31 io workers, 31 writer workers, on a 32-core box.
# Then QWP on the same server, so the comparison is finally same-build.
set -euo pipefail

sudo docker rm -f questdb >/dev/null 2>&1 || true
sudo rm -rf /home/ubuntu/qdbroot
mkdir -p /home/ubuntu/qdbroot
sudo docker run -d --name questdb --network host \
  -e QDB_LINE_TCP_IO_WORKER_COUNT=31 \
  -e QDB_LINE_TCP_WRITER_WORKER_COUNT=31 \
  -v /home/ubuntu/qdbroot:/var/lib/questdb \
  questdb/questdb:nightly
curl -s --retry 90 --retry-delay 2 --retry-all-errors -o /dev/null -w "ping:%{http_code}\n" http://127.0.0.1:9000/ping
sleep 5
curl -s -G --data-urlencode "query=select build" http://127.0.0.1:9000/exec
echo
QPID=$(pgrep -f questdb | head -1)
echo "thread pools:"
ps -L -o comm= -p "$QPID" | sed 's/_[0-9]*$//' | sort | uniq -c | sort -rn | head -6
echo
python3 -u /home/ubuntu/samebuild.py
