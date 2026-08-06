#!/usr/bin/env bash
setsid nohup bash /home/ubuntu/runwal.sh > /home/ubuntu/wal.log 2>&1 < /dev/null &
echo "launched, pid $!"
