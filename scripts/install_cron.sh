#!/bin/bash

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SCHEDULER_SCRIPT="$SCRIPT_DIR/scheduler.py"
PYTHON_PATH=$(which python3)
CRON_JOB="25 16 * * * $PYTHON_PATH $SCHEDULER_SCRIPT >> $SCRIPT_DIR/scheduler_cron.log 2>&1"

if crontab -l 2>/dev/null | grep -q "$SCHEDULER_SCRIPT"; then
    crontab -l 2>/dev/null | grep -v "$SCHEDULER_SCRIPT" | crontab -
fi

(crontab -l 2>/dev/null; echo "$CRON_JOB") | crontab -

echo "✓ Scheduler installed (runs daily at 9:55 PM IST)"
