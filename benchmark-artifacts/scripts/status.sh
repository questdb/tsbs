#!/usr/bin/env bash
# Is the query benchmark making progress?
echo "=== query files generated so far ==="
ls -l /home/ubuntu/queries 2>/dev/null | tail -20
echo
echo "=== running tsbs processes ==="
pgrep -a tsbs_generate_queries || true
pgrep -a tsbs_run_queries_questdb || true
pgrep -a qbench || true
echo
echo "=== python driver ==="
pgrep -af "qbench.py" || true
echo
echo "=== load average ==="
uptime
