#!/usr/bin/env bash
setsid nohup bash /home/ubuntu/run31.sh > /home/ubuntu/samebuild.log 2>&1 < /dev/null &
echo "launched, pid $!"
