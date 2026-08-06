#!/usr/bin/env bash
# Which query type and transport is running right now?
echo "=== full command lines ==="
pgrep -af tsbs_run || echo "no query runner active at this instant"
echo
echo "=== cpu of the runner ==="
ps -eo pid,etime,pcpu,args --sort=-pcpu | head -6
echo
echo "=== questdb cpu ==="
top -b -n 1 | grep -E "^%Cpu|java" | head -3
