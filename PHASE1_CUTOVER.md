# BankUpdate Phase 1 Cutover Notes

## Purpose

This note covers the Phase 1 live cutover for BankUpdate after the new SQLite-backed scheduler-owned flow is deployed.

## New scheduler ownership

After Phase 1, BankUpdate should run from its own dedicated Windows Task Scheduler entries:

- `BankUpdate Daily Main`
- `BankUpdate Daily Fallback`
- `BankUpdate Retry Hourly`

The scheduler-facing command remains:

- `D:\Irshad\Dev\Python\BankUpdate\run_sync.bat`

Supported modes:

- `daily`
- `retry`
- `bootstrap`
- `health`

## Important dependency discovered

Before this change, BankUpdate was not scheduled directly.

The old production chain was:

1. Windows scheduled task `\Grist_Inv`
2. `D:\Irshad\Dev\Python\InvoiceDataExtractor\InvoiceLoader.bat`
3. `D:\Irshad\Dev\Python\InvoiceDataExtractor\wrapper.py`
4. wrapper-managed call to `D:\Irshad\Dev\Python\BankUpdate\run_sync.bat`

That wrapper integration still exists in code for rollback safety, but it is now feature-gated and should be disabled during cutover.

## Pre-cutover checklist

1. Confirm the new BankUpdate code is deployed in `D:\Irshad\Dev\Python\BankUpdate`.
2. Confirm `runtime\db\bankupdate.sqlite3` exists after bootstrap or first initialization.
3. Run:

```bat
run_sync.bat health
```

4. If SQLite is empty, run:

```bat
run_sync.bat bootstrap
```

5. Confirm the invoice workflow is still healthy before touching wrapper configuration.

## Dedicated task schedule

Recommended Phase 1 schedule:

- `BankUpdate Daily Main`: every day at `11:30`
- `BankUpdate Daily Fallback`: every day at `13:00`
- `BankUpdate Retry Hourly`: every `60 minutes`

Notes:

- both daily tasks call `run_sync.bat daily`
- the app skips the duplicate daily run automatically if a successful daily run already happened that day
- the retry task calls `run_sync.bat retry`
- the file lock prevents overlap between daily and retry runs

## Cutover steps

1. Let the old BankUpdate trigger complete one final successful cycle if practical.
2. Create the dedicated BankUpdate scheduled tasks.
3. Disable the old wrapper-driven BankUpdate trigger by editing:

`D:\Irshad\Dev\Python\InvoiceDataExtractor\.env`

Set:

```env
ENABLE_BANKUPDATE_SYNC=false
```

4. Restart the invoice wrapper so it reloads `.env`.

Ways to do that safely:

- restart the `\Grist_Inv` task
- or reboot the machine during a planned maintenance window

5. Run a validation check manually:

```bat
run_sync.bat daily --force
```

6. Confirm only the dedicated BankUpdate tasks are now responsible for BankUpdate execution.

## Post-cutover validation

Check these items:

1. `runtime\db\bankupdate.sqlite3` exists and is updating.
2. `runtime\logs\bankupdate.log` shows the new run entries.
3. `runtime\reports\` contains any manual-review reports if exceptions were detected.
4. `\Grist_Inv` is still running for invoice processing, but not launching BankUpdate anymore.
5. `wrapper.log` in `InvoiceDataExtractor` shows:
   `BankUpdate wrapper integration is disabled.`

## Rollback

If the dedicated BankUpdate tasks need to be rolled back:

1. Disable the dedicated BankUpdate tasks.
2. Edit:

`D:\Irshad\Dev\Python\InvoiceDataExtractor\.env`

Set:

```env
ENABLE_BANKUPDATE_SYNC=true
```

3. Restart the invoice wrapper or reboot the machine.
4. Confirm `wrapper.log` shows:
   `BankUpdate wrapper integration is enabled.`

## Current implementation notes

- the new launcher no longer uses `pause`
- the new launcher returns scheduler-friendly exit codes
- lock contention returns exit code `10`
- the new runtime stores state under `runtime\`
