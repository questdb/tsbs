#!/usr/bin/env bash
# Launch the sweep detached so a dropped SSH connection cannot kill it.
setsid nohup bash /home/ubuntu/run_sweep.sh > /home/ubuntu/sweep.log 2>&1 < /dev/null &
echo "launched, pid $!"
