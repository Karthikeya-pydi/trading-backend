#!/bin/bash

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SCHEDULER_SCRIPT="$SCRIPT_DIR/scheduler.py"
PYTHON_PATH=$(which python3)
# Cron job runs at 10:00 PM IST (4:30 PM UTC)
# If server is in IST timezone, change to "0 22 * * *"
CRON_JOB="30 16 * * * $PYTHON_PATH $SCHEDULER_SCRIPT >> $SCRIPT_DIR/scheduler_cron.log 2>&1"

if crontab -l 2>/dev/null | grep -q "$SCHEDULER_SCRIPT"; then
    crontab -l 2>/dev/null | grep -v "$SCHEDULER_SCRIPT" | crontab -
fi

(crontab -l 2>/dev/null; echo "$CRON_JOB") | crontab -

echo "✓ Scheduler installed (runs daily at 10:00 PM IST)"
