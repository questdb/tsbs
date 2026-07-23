#!/usr/bin/env bash
# Rebuild with the binary-input fix and rerun the full three-transport matrix.
set -euo pipefail
export PATH=$PATH:/usr/local/go/bin
cd /home/ubuntu/tsbs
make tsbs_load_questdb
python3 /home/ubuntu/bench3.py 32 2
