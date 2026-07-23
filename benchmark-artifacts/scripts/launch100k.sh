#!/usr/bin/env bash
setsid nohup bash /home/ubuntu/run100k.sh > /home/ubuntu/100k.log 2>&1 < /dev/null &
echo "launched, pid $!"
