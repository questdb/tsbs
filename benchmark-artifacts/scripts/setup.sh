#!/usr/bin/env bash
# Prerequisites for the TSBS QWP/ILP benchmark, per the tsbs-benchmark skill.
set -euo pipefail

echo "=== apt prerequisites ==="
sudo apt-get update -qq
sudo apt-get install -y -qq ca-certificates curl make gcc gzip

echo "=== docker ==="
if ! command -v docker >/dev/null 2>&1; then
  sudo install -m 0755 -d /etc/apt/keyrings
  sudo curl -fsSL https://download.docker.com/linux/ubuntu/gpg -o /etc/apt/keyrings/docker.asc
  sudo chmod a+r /etc/apt/keyrings/docker.asc
  . /etc/os-release
  echo "deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/docker.asc] https://download.docker.com/linux/ubuntu ${VERSION_CODENAME} stable" | sudo tee /etc/apt/sources.list.d/docker.list >/dev/null
  sudo apt-get update -qq
  sudo apt-get install -y -qq docker-ce docker-ce-cli containerd.io docker-buildx-plugin
fi
sudo docker --version

echo "=== go ==="
NEED_GO=1
if command -v /usr/local/go/bin/go >/dev/null 2>&1; then
  CUR=$(/usr/local/go/bin/go version | awk '{print $3}')
  echo "found $CUR"
  NEED_GO=0
fi
if [ "$NEED_GO" = "1" ]; then
  GO_VERSION=$(curl -fsSL "https://go.dev/VERSION?m=text" | head -1)
  GO_ARCH=$(dpkg --print-architecture)
  echo "installing ${GO_VERSION} for ${GO_ARCH}"
  curl -fsSL "https://go.dev/dl/${GO_VERSION}.linux-${GO_ARCH}.tar.gz" -o /tmp/go.tar.gz
  sudo rm -rf /usr/local/go
  sudo tar -C /usr/local -xzf /tmp/go.tar.gz
  rm /tmp/go.tar.gz
fi
/usr/local/go/bin/go version

echo "=== tsbs ==="
export PATH=$PATH:/usr/local/go/bin:$HOME/go/bin
if [ ! -d /home/ubuntu/tsbs ]; then
  git clone --quiet --branch jv/adding_qwp https://github.com/questdb/tsbs.git /home/ubuntu/tsbs
fi
cd /home/ubuntu/tsbs
git fetch --quiet origin jv/adding_qwp
git checkout --quiet FETCH_HEAD
git log --oneline -1
make tsbs_generate_data tsbs_generate_queries tsbs_load_questdb tsbs_run_queries_questdb
ls -l /home/ubuntu/tsbs/bin

echo "=== questdb nightly ==="
sudo docker rm -f questdb >/dev/null 2>&1 || true
sudo docker pull -q questdb/questdb:nightly
sudo docker run -d --name questdb \
  -p 9000:9000 -p 9009:9009 -p 8812:8812 -p 9003:9003 \
  -v /home/ubuntu/qdbroot:/var/lib/questdb \
  questdb/questdb:nightly
curl -s --retry 60 --retry-delay 2 --retry-all-errors -o /dev/null -w "ping:%{http_code}\n" http://127.0.0.1:9000/ping
curl -s -G --data-urlencode "query=select build" http://127.0.0.1:9000/exec
echo
echo "=== setup complete ==="
