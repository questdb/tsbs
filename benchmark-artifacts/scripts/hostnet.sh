#!/usr/bin/env bash
# The first A/B ran QuestDB with Docker port mapping, so every byte crossed
# docker-proxy. ILP pushes 24GB where QWP pushes 7GB, so that penalises ILP
# unfairly. Restart on host networking and measure again.
set -euo pipefail

echo "=== restarting questdb with host networking ==="
sudo docker rm -f questdb >/dev/null 2>&1 || true
sudo docker run -d --name questdb --network host \
  -v /home/ubuntu/qdbroot:/var/lib/questdb \
  questdb/questdb:nightly
curl -s --retry 60 --retry-delay 2 --retry-all-errors -o /dev/null -w "ping:%{http_code}\n" http://127.0.0.1:9000/ping
curl -s -G --data-urlencode "query=drop table if exists cpu" http://127.0.0.1:9000/exec
echo
echo "=== cpu count seen by the container ==="
sudo docker exec questdb nproc
echo "=== rerun ==="
python3 /home/ubuntu/bench.py 32 2
