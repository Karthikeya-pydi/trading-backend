# Scheduler Status Check

## Is it running?

```bash
# Check logs
tail -30 scripts/scheduler.log

# Check errors
tail -30 scripts/scheduler_cron.log
```

## Schedule

- **Runs at**: 10:00 PM IST daily
- **Status**: Installed ✓

## Test now

```bash
python3 scripts/scheduler.py
```
