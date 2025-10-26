# Scheduler Status Check

## Is it running?

```bash
# Check logs
tail -30 scripts/scheduler.log

# Check errors
tail -30 scripts/scheduler_cron.log
```

## Schedule

- **Runs at**: 9:35 PM daily (server time)
- **Status**: Installed ✓

## Test now

```bash
python3 scripts/scheduler.py
```
