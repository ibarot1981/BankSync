# Invoice Wrapper Change Notes

## Scope

This file documents the cross-project change made in:

- `D:\Irshad\Dev\Python\InvoiceDataExtractor\wrapper.py`

The invoice project itself is outside the BankUpdate implementation scope, so this note records the exact change and the intended boundary.

## Change made

Added a feature flag:

- `ENABLE_BANKUPDATE_SYNC`

Behavior:

- when `ENABLE_BANKUPDATE_SYNC=true`:
  - `wrapper.py` keeps its previous BankUpdate behavior
  - it continues to trigger `RUN_SYNC_BAT_PATH` on its internal schedule
- when `ENABLE_BANKUPDATE_SYNC=false`:
  - `wrapper.py` skips only the BankUpdate trigger
  - invoice extractor behavior, Grist uploader behavior, and consignee update behavior remain unchanged

## Why this change was made

BankUpdate Phase 1 now owns its own dedicated Task Scheduler jobs.

Without this gate, BankUpdate would still be triggered from the invoice wrapper and would have two active scheduling paths:

1. the new dedicated BankUpdate tasks
2. the old wrapper-driven hook

That would violate the Phase 1 cutover rule that BankUpdate must have exactly one active production scheduling path.

## What was intentionally not changed

These wrapper behaviors were left untouched:

- `claude_InvDataEx.py` startup and restart logic
- periodic `grist_uploader.py` execution
- periodic `update_consignee_from_transactions.py` execution
- Grist availability checks
- boot-time launch model from `InvoiceLoader.bat`
- internal upload and transaction-update intervals

## Operational use

During cutover:

- set `ENABLE_BANKUPDATE_SYNC=false` in `InvoiceDataExtractor\.env`

During rollback:

- set `ENABLE_BANKUPDATE_SYNC=true` in `InvoiceDataExtractor\.env`

The wrapper must be restarted after changing `.env`.
