# BankUpdate Implementation Workplan

## Purpose

This is the working document for planning and implementing the next version of BankUpdate.

It serves four purposes:

1. capture confirmed business and technical decisions
2. track unanswered questions
3. define the implementation sequence
4. act as the handoff reference during implementation

This document will be updated continuously as questions are answered and implementation progresses.

## Current Status

- status: implementation-planned
- implementation started: no
- primary reference: `IMPROVEMENT_PLAN.md`
- handoff readiness: this document is intended to be sufficient to resume implementation without relying on chat history, subject to live environment availability

## Confirmed Requirements

These requirements are already confirmed.

### Business requirements

- Google Sheets is the upstream source of bank transaction updates.
- Grist is the downstream destination.
- No records must be missed.
- No duplicate records must be created.
- Performance should stay stable as data volume grows.
- The design target should comfortably support at least 5x current observed scale.
- Grist is not always available and this must be handled safely.
- Existing historical data in Grist must not be modified.
- Going forward, the system can be treated as append-only.
- Audit trail must be maintained.
- The `.csv` intermediate stage can be removed if the replacement is safer and better aligned with the requirements.
- If SQLite is empty on first run or after moving to a new system, there must be a supported initialization path.
- In that initialization path, Grist must be used to seed SQLite first.
- After seeding from Grist, Google Sheets must be checked for records not yet represented in local state.

### Observed data scale from current repository

- 2025 archived snapshots: about 58,604 rows across 70 files
- 2026 archived snapshots so far: about 34,456 rows across 21 files
- recent daily file sizes: roughly 1,600 to 2,100 rows
- largest observed daily snapshot: 2,113 rows

### Agreed direction so far

- Do not use Grist as the primary source of sync truth.
- Maintain local durable sync state.
- Prefer SQLite as the local durable store.
- Prefer fingerprint-based deduplication over row-number-only deduplication.
- Remove CSV staging in the target design.
- Preserve raw audit evidence plus structured delivery state.
- Use a migration cutover so historical data is not reprocessed into Grist.
- Support bootstrap-from-Grist when SQLite is empty on a fresh machine or environment.

## Working Solution Direction

The current intended design is:

1. fetch rows from Google Sheets
2. normalize them into canonical transaction form
3. compute transaction fingerprint
4. store transaction and delivery state in SQLite
5. queue undelivered records locally
6. deliver queued records to Grist when available
7. keep audit history for fetch, dedupe, queue, and delivery events

Special case:

- if SQLite is empty, seed it from Grist first, then continue with Google Sheets ingestion

## Open Questions

These are the questions still needing explicit answers before implementation.

### Q1. Transaction identity definition

We need to finalize which fields define the business identity of a transaction for fingerprinting.

Candidate fields:

- bank
- transaction date
- transaction amount
- transaction description
- reference number
- value date

Answered.

### Q2. Google Sheets usage pattern

We need to confirm how the sheet is maintained operationally.

Key points to clarify:

- are new rows always appended at the bottom?
- are rows ever inserted in the middle?
- are historical rows ever edited?
- are rows ever sorted or rearranged?

Partially answered.

### Q3. Historical correction policy

If a historical row in Google Sheets changes after cutover, we need to know how to treat it.

Possible policies:

- ignore all pre-cutover rows forever
- ignore all historical edits after cutover
- treat materially changed rows as new transactions

Answered.

### Q4. Audit retention policy

We need to define how long to retain:

- raw snapshots
- SQLite operational history
- logs

Partially answered.

### Q5. Retry model

We need to decide whether retry of failed Grist deliveries should happen:

- during every normal scheduled run
- through a separate retry command or job
- both

Answered.

### Q6. Operational deployment model

We need to understand how this is run in production today and how much change is acceptable.

Questions:

- is it always run on one Windows machine?
- is Task Scheduler the intended long-term scheduler?
- do we need to preserve the existing bat/script entrypoint during transition?

Partially answered.

### Q7. Grist bootstrap field completeness

For seeding SQLite from Grist, we need to confirm Grist contains all fields needed to reconstruct the transaction fingerprint reliably.

Key points to confirm:

- does Grist contain all identity-defining fields?
- is `GSheets_RowNum` present and reliable enough to seed the initial checkpoint?
- are any fields transformed in Grist in a way that would make fingerprint reproduction ambiguous?

Partially answered.

## Answer Log

This section records answers as they are confirmed.

### Answer 1

Grist connection has been verified from the current project configuration.

Verified:

- document access succeeded
- table access succeeded
- column inspection succeeded
- sample-record read succeeded

Verified Grist table:

- host: configured and reachable
- document: `gi1sPNycQAHoMTekxE6QN3`
- table: `BankReceipts`

Verified field availability in Grist:

- `Bank`
- `Transaction_Date`
- `Transaction_Amount`
- `Transaction_Description`
- `Reference_No`
- `Value_Date`
- `GSheets_RowNum`

Important note:

The Grist schema uses underscore-style field names rather than the Google Sheets header names. For bootstrap and fingerprint reproduction this is acceptable, because the required business fields do appear to be present, but the normalization/mapping layer must treat these as canonical equivalents.

Still not fully answered:

- whether these fields are always populated consistently enough for fingerprinting
- whether `GSheets_RowNum` has complete historical coverage and is reliable enough to seed the checkpoint without fallback logic

### Answer 2

Data quality and business semantics for source fields:

- `Reference No.` comes from the bank as-is
- `Transaction Description` comes from the bank as-is
- either field may be blank, truncated, or less reliable depending on what the bank provides
- `Value Date` is the key business date field
- `Transaction Date` should still be captured even though it is less important than `Value Date`

Implication:

- fingerprint design should not assume `Reference No.` or `Transaction Description` are always complete
- fingerprint design should likely prioritize `Value Date` over `Transaction Date`

### Answer 3

Google Sheets header situation:

- the field previously known as `Transaction Date` in Google Sheets is currently named `Column 1`
- this appears to be due to an accidental header change in Google Sheets
- the user does not want to edit the Google Sheets header right now
- in Grist, this field is still represented as transaction date

Implication:

- the Google Sheets ingestion layer must support header aliasing
- `Column 1` must be treated as the source alias for `Transaction Date`
- the implementation must avoid depending on perfect sheet headers

### Answer 4

Bank-specific date parsing rule:

- for HDFC bank rows, `Transaction Date` is in `dd-mm-yyyy` format and includes time
- for ICICI bank rows, `Transaction Date` is in `mm-dd-yyyy` format and includes time
- `Value Date` is always in `dd-mm-yyyy` format across banks

Implication:

- normalization must parse `Transaction Date` using bank-specific rules
- normalization can parse `Value Date` using one common rule
- fingerprint logic should rely primarily on normalized `Value Date`, with normalized `Transaction Date` retained as supporting metadata

### Answer 5

Working fingerprint definition:

The fingerprint should use:

- `Bank`
- `Value Date`
- `Transaction Amount`
- `Reference No.`
- `Transaction Description`
- `Transaction Date`

Clarifications:

- `Reference No.` remains part of the fingerprint even when blank
- `Transaction Date` remains part of the fingerprint because it includes time and helps distinguish otherwise similar transactions
- `Value Date` does not have a timestamp, so `Transaction Date` helps differentiate transactions that share bank, value date, amount, and weak or blank reference data
- this means the fingerprint must be computed from normalized values, not raw strings

### Answer 6

Google Sheets operational behavior:

- as normal daily behavior, new rows are appended at the bottom
- in rare cases, a missing earlier-day row may be added in the middle
- this middle insertion has happened only once so far
- rows are not sorted
- rows are not rearranged

Implication:

- row-number checkpointing remains viable as the fast-path strategy
- the implementation should still use a replay window and fingerprint-based dedupe to safely absorb rare mid-sheet insertions

Still not fully answered:

- whether historical rows are ever edited after being entered

### Answer 7

Historical row edit policy:

- as a rule, historical rows are not edited
- however, if a mistake is noticed, a historical row may be corrected

Implication:

- the system should be designed as append-mostly, not strictly append-only at the sheet level
- normal incremental ingestion can still rely on row checkpointing plus replay window
- periodic reconciliation is still needed as a safety mechanism for rare historical corrections
- because the business requirement says existing Grist data should not be modified going forward, historical sheet corrections after delivery will need a documented handling policy rather than automatic in-place Grist updates

### Answer 8

Historical correction handling policy:

- if a historical row in Google Sheets is corrected after it has already been delivered to Grist, it should be flagged for manual review

Implication:

- the system must not automatically update existing Grist records
- the system must not automatically insert a corrected historical row as a new transaction just because its fingerprint changes
- reconciliation logic should be able to detect this condition and record it as an exception for operator review
- the implementation should include a review/audit mechanism for these exceptions

### Answer 9

Audit snapshot direction:

- full Google Sheets snapshots do not need to be retained routinely
- the user does not want full-source snapshot retention as a normal audit strategy
- this is acceptable because the source sheet can be fetched again if needed

Working recommendation adopted:

- use delta snapshots for normal audit retention
- optionally allow occasional full snapshots only for special troubleshooting or controlled recovery situations, not as standard behavior

Implication:

- audit storage should focus on:
  - fetched delta rows
  - normalized transaction state
  - delivery/audit events
  - exception records

### Answer 10

Retention policy:

- SQLite history/state retention target: not more than 1 month
- application log rotation target: 5 log files, about 5 MB each

Implication:

- the implementation should include a retention cleanup strategy for SQLite audit/history tables
- core operational state still needs to remain sufficient for correctness, so retention cleanup must distinguish:
  - required current sync state
  - old audit/history records eligible for pruning
- log rotation settings can follow the current general pattern:
  - max bytes: about 5 MB
  - backup count: 5

### Answer 11

Retry behavior:

- retry of failed Grist deliveries should happen in both ways:
  - during every normal scheduled run
  - through a separate retry command or job

Implication:

- the normal pipeline should always attempt delivery of pending/retry items
- there should also be an explicit retry mode for operational recovery or catch-up after an outage

### Answer 12

Production run model:

- the system should run on one Windows machine
- Windows Task Scheduler is the intended scheduler
- the design should support moving to another machine later
- however, only one machine should run the system at any given point in time

Implication:

- the implementation should remain Windows-friendly
- machine migration/bootstrap must be a supported operational workflow
- the design should avoid any assumption of active multi-machine concurrency
- documentation should include a controlled machine-switch procedure

### Answer 13

Transition launcher requirement:

- keep a `.bat` launcher during transition for Task Scheduler compatibility

Implication:

- the new implementation should expose a stable command that can be called from a bat file
- the bat launcher should remain the scheduler-facing entrypoint even if the internal Python execution model changes

### Answer 14

Live Grist bootstrap-quality inspection:

Verified from live Grist data:

- total records inspected: 2,132
- `GSheets_RowNum` present for all inspected records
- `GSheets_RowNum` parseable as integer for all inspected records
- observed `GSheets_RowNum` range: 2 to 2114

Field completeness caveats from Grist:

- `Transaction_Amount` missing/blank: 0
- `Reference_No` missing/blank: 54
- `Transaction_Date` missing/blank: 722
- `Bank` missing/blank: 1
- `Transaction_Description` missing/blank: 1
- `Value_Date` missing/blank: 1

Observed data-quality caveat:

- some `Bank` values appear as `C` or `c` instead of expected bank names

Implication:

- Grist is usable as the initial seed source
- `GSheets_RowNum` appears strong enough to help initialize checkpoint state
- however, Grist alone may not be sufficient to reconstruct the agreed fingerprint for all historical rows, because many rows have blank `Transaction_Date`
- bootstrap design may need a second enrichment step from Google Sheets for historical rows where fingerprint-driving fields are incomplete in Grist

### Answer 15

Bootstrap enrichment policy:

- bootstrap from Grist is approved
- if a seeded Grist row is missing fingerprint-driving fields such as `Transaction_Date`, bootstrap should enrich that row from Google Sheets using `GSheets_RowNum`

Implication:

- the empty-SQLite initialization flow becomes:
  - seed baseline records from Grist
  - identify incomplete seeded records
  - enrich incomplete records from Google Sheets by row number
  - compute final fingerprints after enrichment
  - initialize checkpoint state
- bootstrap logic must support partial enrichment rather than assuming Grist is fully self-sufficient

### Answer 18

Live Google Sheets access verification:

Verified from the current project credentials and environment:

- spreadsheet access succeeded
- worksheet access succeeded
- sheet data read succeeded

Verified sheet details:

- spreadsheet title: `Payment Receipt`
- worksheet title: `Payment Receipt`
- observed row count: 2114
- observed header count: 30

Verified key source headers:

- `Column 1`
- `Transaction Description`
- `Transaction Amount`
- `Bank`
- `Reference No.`
- `Value Date`
- `Running Balance`

Important implementation note:

- live sheet values were observed in slash-style date text forms such as:
  - `01/04/2025 13:02:54`
  - `1/4/25`
- implementation should normalize based on actual incoming values, not only on intended display conventions or header names

### Answer 16

Main scheduler timing:

- main Task Scheduler job should run once a day
- target run time: every day at 11:30 AM
- fallback run time if the main time is missed: 1:00 PM

Implication:

- the operational schedule is daily, not high-frequency intraday
- the implementation should support an explicit fallback scheduler entry or equivalent scheduled recovery run
- retry design becomes more important because new delivery attempts are not happening continuously throughout the day

### Answer 17

Retry scheduler timing:

- if Grist delivery is pending, retry should be attempted every 60 minutes

Implication:

- a separate retry job should exist
- that retry job should run hourly
- the retry job should only process queued pending/retry records and should not perform full Google Sheets ingestion

### Answer 19

Live production trigger-chain verification:

Verified on the current Windows machine:

- the only live scheduled task relevant to this flow is `\Grist_Inv`
- `\Grist_Inv` does not run BankUpdate directly
- `\Grist_Inv` is a boot-triggered task that starts `D:\Irshad\Dev\Python\InvoiceDataExtractor\InvoiceLoader.bat`
- `InvoiceLoader.bat` starts `wrapper.py` in the `InvoiceDataExtractor` project
- `wrapper.py` reads `RUN_SYNC_BAT_PATH` from the `InvoiceDataExtractor` `.env`
- that path currently points to `D:\Irshad\Dev\Python\BankUpdate\run_sync.bat`
- `wrapper.py` triggers BankUpdate on an internal clock at `11:30` and `16:00`

Important clarification:

- BankUpdate is currently not scheduled twice daily by Windows Task Scheduler itself
- instead, Windows Task Scheduler starts the long-running invoice wrapper at boot, and that wrapper triggers BankUpdate later

Confirmed Phase 1 direction:

- BankUpdate should be decoupled from `InvoiceDataExtractor`
- BankUpdate should own its own dedicated Task Scheduler jobs
- the old wrapper-based BankUpdate trigger should be removed or disabled during cutover
- the `\Grist_Inv` task should remain in place for invoice processing unless there is a separate invoice-project change

Implication:

- Phase 1 scope includes a controlled cross-project cutover dependency
- the BankUpdate cutover must update both:
  - the BankUpdate scheduler-facing launcher
  - the `InvoiceDataExtractor` wrapper configuration or code so it no longer triggers BankUpdate
- cutover and rollback steps must distinguish:
  - invoice scheduler continuity
  - BankUpdate scheduler ownership

## Implementation Assumptions

These are the current working assumptions for implementation. Any assumption not yet explicitly confirmed remains visible.

### Assumption A1. SQLite retention split

The 1-month retention target will be interpreted as applying to:

- run history
- audit events
- exception logs
- retry history details

But not to the minimum dedupe ledger required for correctness.

Working implementation assumption:

- detailed operational history is pruned after about 1 month
- the minimum fingerprint/state ledger needed to prevent duplicates is retained longer as required

Reason:

- pruning the dedupe ledger after 1 month would weaken duplicate prevention
- this would conflict with the core requirement of no duplicates

Status:

- not yet explicitly confirmed
- implementation can proceed with this as the safe assumption

### Assumption A2. Bank-value cleanup

Observed Grist data includes some `Bank` values like `C` and `c`.

Working implementation assumption:

- unexpected bank values will be ingested as-is
- normalization can flag them as data-quality warnings or manual-review candidates if needed

Status:

- non-blocking

## Technical Design

### Target runtime model

The new version will run as a Python application with a Windows `.bat` launcher in front of it.

The `.bat` file remains the Task Scheduler-facing entrypoint.

For live operation after Phase 1:

- BankUpdate should have its own dedicated Task Scheduler jobs
- BankUpdate should not depend on the `InvoiceDataExtractor` long-running wrapper for scheduling
- any temporary compatibility with `RUN_SYNC_BAT_PATH` should be treated only as a transition mechanism during cutover

Internally, the application should support these modes:

- `daily`
- `retry`
- `bootstrap`
- `reconcile`
- `health`

### Target command behavior

#### `daily`

Purpose:

- seed SQLite from Grist if SQLite is empty
- fetch Google Sheets delta
- normalize rows
- compute fingerprints
- store new transactions in SQLite
- queue undelivered records
- attempt Grist delivery for pending items
- record run summary

#### `retry`

Purpose:

- process only pending/retry delivery queue items
- do not fetch Google Sheets rows
- do not advance Google Sheets checkpoint

#### `bootstrap`

Purpose:

- initialize empty SQLite from Grist
- enrich incomplete seeded records from Google Sheets using `GSheets_RowNum`
- compute fingerprints
- initialize local checkpoint state

This command may be used:

- automatically by `daily` when SQLite is empty
- manually during machine migration or controlled recovery

#### `reconcile`

Purpose:

- inspect a replay range or reconciliation window in Google Sheets
- detect historical corrections or unusual insertions
- flag issues for manual review
- do not automatically modify historical Grist data

This is not required for the first live cutover, but should exist soon after.

#### `health`

Purpose:

- verify access to Google Sheets
- verify access to Grist
- verify SQLite integrity/basic readiness

### Proposed project structure

```text
BankUpdate/
  IMPLEMENTATION_WORKPLAN.md
  IMPROVEMENT_PLAN.md
  requirements.txt
  run_sync.bat
  src/
    bankupdate/
      __init__.py
      cli.py
      config.py
      logging_utils.py
      paths.py
      sqlite_store.py
      google_sheets_client.py
      grist_client.py
      normalization.py
      fingerprint.py
      ingestion.py
      delivery.py
      bootstrap.py
      reconcile.py
      retention.py
      locking.py
      models.py
  runtime/
    logs/
    snapshots/
    db/
    reports/
```

### Proposed SQLite schema

#### `app_state`

Purpose:

- singleton key/value table for runtime state

Suggested fields:

- `key`
- `value`
- `updated_at`

Expected keys:

- `schema_version`
- `cutover_row`
- `last_ingested_row`
- `last_bootstrap_at`
- `last_daily_run_at`
- `last_retry_run_at`

#### `transactions`

Purpose:

- store unique normalized business transactions

Suggested fields:

- `fingerprint` primary key
- `bank`
- `value_date`
- `transaction_date`
- `transaction_amount`
- `reference_no`
- `transaction_description`
- `source_row_num`
- `source_snapshot_date`
- `source_origin`
- `grist_seeded`
- `created_at`
- `normalized_payload_json`

Notes:

- `source_origin` can be `grist_seed`, `sheet_delta`, or `sheet_enrichment`
- this table is the core dedupe ledger and should not be pruned casually

#### `delivery_queue`

Purpose:

- track Grist delivery state per transaction

Suggested fields:

- `fingerprint` primary key
- `status`
- `attempt_count`
- `first_queued_at`
- `last_attempt_at`
- `last_error`
- `delivered_at`
- `grist_record_id`

Suggested statuses:

- `pending`
- `retry`
- `delivered`
- `manual_review`

#### `audit_events`

Purpose:

- append-only operational audit trail

Suggested fields:

- `event_id` integer primary key
- `run_id`
- `fingerprint`
- `event_type`
- `event_time`
- `details_json`

Expected retention:

- prune older than about 1 month

#### `run_history`

Purpose:

- summarize each application run

Suggested fields:

- `run_id` primary key
- `run_type`
- `started_at`
- `completed_at`
- `status`
- `fetched_count`
- `new_count`
- `queued_count`
- `delivered_count`
- `retry_count`
- `manual_review_count`
- `error_summary`

Expected retention:

- prune older than about 1 month

#### `manual_review`

Purpose:

- hold reconciliation and correction exceptions that need a human decision

Suggested fields:

- `review_id` integer primary key
- `fingerprint`
- `source_row_num`
- `issue_type`
- `detected_at`
- `status`
- `details_json`

### Proposed fingerprint algorithm

Fingerprint input should be built from normalized canonical values in this exact field order:

1. `Bank`
2. `Value Date`
3. `Transaction Amount`
4. `Reference No.`
5. `Transaction Description`
6. `Transaction Date`

Normalization rules before hashing:

- trim strings
- normalize blank strings to empty values consistently
- normalize date fields to canonical text forms
- normalize amount to a canonical decimal string
- preserve blank `Reference No.` as a deliberate part of the fingerprint input

Suggested hashing approach:

- build a deterministic pipe-delimited canonical string
- hash with SHA-256

### Proposed date normalization rules

#### `Transaction Date`

- HDFC: parse as `dd-mm-yyyy` with time
- ICICI: parse as `mm-dd-yyyy` with time
- store as canonical timestamp string for fingerprinting and SQLite storage

#### `Value Date`

- parse as `dd-mm-yyyy` for all banks
- store as canonical date string for fingerprinting and SQLite storage

### Proposed Google Sheets header mapping

Working alias mapping:

- `Column 1` -> `Transaction Date`
- `Transaction Date` -> `Transaction Date`
- `Transaction Description` -> `Transaction Description`
- `Transaction Amount` -> `Transaction Amount`
- `Bank` -> `Bank`
- `Reference No.` -> `Reference No.`
- `Value Date` -> `Value Date`
- `Running Balance` -> `Running Balance`

### Proposed checkpoint strategy

Normal ingestion should use a hybrid checkpoint model:

- fast-path checkpoint based on `last_ingested_row`
- replay window for safety
- fingerprint uniqueness as the final correctness guarantee

Working default:

- replay the last 200 rows on each daily run

Behavior:

- read from `max(2, last_ingested_row - 200 + 1)`
- process those rows plus anything newer
- let SQLite dedupe by fingerprint
- after successful SQLite commit, advance `last_ingested_row`

### Proposed bootstrap strategy

If SQLite is empty:

1. read all relevant rows from Grist
2. map Grist fields into canonical transaction form
3. identify rows missing fingerprint-driving fields
4. enrich those rows from Google Sheets using `GSheets_RowNum`
5. compute fingerprints
6. insert `transactions`
7. insert `delivery_queue` rows as already `delivered`
8. set `last_ingested_row` to the highest reliable seeded `GSheets_RowNum`
9. continue with normal daily delta logic

### Proposed delivery strategy

- upload only queue items with `pending` or `retry`
- deliver in batches/chunks
- mark delivered only after confirmed Grist success
- leave rows queued if Grist is unavailable
- retry on:
  - every `daily` run
  - every hourly `retry` run

### Proposed scheduler design

#### Job 1: Daily main run

- schedule: every day at 11:30 AM
- fallback schedule: every day at 1:00 PM
- mode: `daily`

#### Job 2: Hourly retry run

- schedule: every 60 minutes
- mode: `retry`
- behavior: no Google Sheets fetch, delivery queue only

#### Scheduler ownership note

- these jobs should be created as dedicated BankUpdate Task Scheduler entries
- the existing `InvoiceDataExtractor` boot task `\Grist_Inv` should not remain the production scheduler for BankUpdate after cutover
- the old wrapper-based call to `RUN_SYNC_BAT_PATH` should be removed or disabled once the dedicated BankUpdate jobs are enabled

### Concurrency control

Because only one machine should run the system at a time, v1 should still prevent accidental overlapping runs on the same machine.

Phase 1 should include:

- a local lock file or SQLite-based run lock
- refusal to start a second active run if one is already in progress

This is especially important because:

- the 1:00 PM fallback could overlap with a delayed 11:30 AM run
- the hourly retry job could overlap with the daily run

## Implementation Plan

The phases below are now implementation-ready.

### Phase 1. Minimal Live Cutover Release

Goal:

- deliver the minimum safe system that can replace the current flow and go live immediately

This phase must include:

- SQLite database and schema
- config loading
- logging setup
- normalization and fingerprinting
- Grist bootstrap for empty SQLite
- Google Sheets delta ingestion using checkpoint + replay window
- direct Grist delivery without CSV staging
- delivery queue with retry state
- `daily` command
- `retry` command
- `.bat` launcher update
- scheduler-compatible exit codes
- single-instance lock
- manual-review exception creation for detected historical corrections
- cutover-safe rule that historical Grist data is never modified

Phase 1 intentionally does not need everything, but it must be production-usable.

#### Phase 1 deliverables

1. new runtime layout and SQLite DB file location
2. new Python package modules
3. bootstrap-from-Grist logic
4. Google Sheets incremental fetch
5. direct queue-to-Grist upload
6. updated `run_sync.bat`
7. operator notes for cutover and rollback
8. cross-project cutover update for `InvoiceDataExtractor` scheduler handoff

#### Phase 1 cutover path

1. run current system one last time
2. deploy new code
3. create dedicated BankUpdate Task Scheduler entries
4. disable or remove the old BankUpdate trigger from `InvoiceDataExtractor\wrapper.py` or its controlling configuration
5. initialize SQLite through bootstrap
6. validate bootstrap and checkpoint
7. run `daily` in validation mode if available
8. confirm only the new BankUpdate scheduler path is active

### Phase 2. Operational Safety and Recovery

Goal:

- harden the live system without changing the core model

This phase should include:

- `health` command
- richer run summaries
- retention cleanup jobs
- improved failure diagnostics
- clearer exception/manual-review reports
- bootstrap validation report

### Phase 3. Reconciliation and Data Quality Controls

Goal:

- detect rare sheet anomalies and historical corrections more deliberately

This phase should include:

- `reconcile` command
- periodic historical window checks
- correction detection logic
- stronger data-quality warnings for unexpected bank values or malformed rows

### Phase 4. Test Coverage and Maintainability

Goal:

- reduce future change risk

This phase should include:

- unit tests
- mocked integration tests
- docs refresh
- machine-switch/bootstrap procedure documentation

## Phase 1 Task Breakdown

This is the concrete coding sequence for the first live-capable phase.

### Task 1. Create package skeleton and runtime paths

- add `src/bankupdate/`
- add runtime path helpers
- keep current scripts untouched until replacement is ready

### Task 2. Add centralized config and logging

- load `.env`
- standardize log file location and rotation
- expose scheduler-friendly command modes

### Task 3. Implement normalization and fingerprinting

- implement bank-specific transaction-date parsing
- implement value-date parsing
- implement amount normalization
- implement canonical fingerprint generation

### Task 4. Implement SQLite store and schema creation

- create DB initialization routine
- create tables
- add indexes
- add state helpers

### Task 5. Implement Grist client

- read records for bootstrap
- write records for delivery
- support chunked upload
- support timeouts and error reporting

### Task 6. Implement Google Sheets client

- fetch rows with header alias mapping
- support row-range fetch for replay window
- support row lookup by `GSheets_RowNum` for bootstrap enrichment

### Task 7. Implement bootstrap flow

- detect empty SQLite
- seed from Grist
- enrich incomplete rows from Google Sheets
- compute fingerprints
- initialize checkpoint state

### Task 8. Implement daily ingestion flow

- acquire run lock
- bootstrap if needed
- fetch replay window + new rows
- insert unseen transactions
- create queue records
- attempt delivery
- record run summary
- release run lock

### Task 9. Implement retry flow

- acquire run lock
- process pending/retry queue rows only
- update delivery status
- record run summary
- release run lock

### Task 10. Update bat launcher and scheduler notes

- preserve `.bat` entrypoint
- route to `daily` or `retry`
- document dedicated Task Scheduler entries for BankUpdate
- document exact disablement of the old wrapper-based BankUpdate trigger

### Task 10A. Update cross-project wrapper handoff

- update `InvoiceLoader.bat` or `wrapper.py` only as needed for BankUpdate decoupling
- remove or gate the `RUN_SYNC_BAT_PATH` trigger used by the invoice wrapper
- preserve invoice-project behavior unrelated to BankUpdate
- keep rollback simple by making the old trigger easy to restore temporarily if required

### Task 11. Prepare cutover procedure

- document one-time switchover steps
- document rollback steps
- document first-run validation checks
- document how to verify that `\Grist_Inv` is no longer triggering BankUpdate

## Pending / Not Yet Explicitly Confirmed

These items were previously pending and are now resolved.

### P1. SQLite retention interpretation

Confirmed:

- 1-month retention applies to audit/run history
- the minimum dedupe ledger may remain longer for correctness

### P2. Manual review operating process

Confirmed:

- manual review items should be surfaced in both ways:
  - SQLite table
  - generated report file

## Implementation Guardrails

These rules should hold throughout implementation.

- No change should modify existing historical Grist records.
- No change should silently weaken duplicate prevention.
- No change should require re-uploading historical transactions.
- New local state must be durable before Grist delivery is attempted.
- Delivery failures must not result in data loss.
- Empty SQLite initialization must seed from Grist before any Google Sheets delivery attempt occurs.
- No cutover step should disable invoice processing unintentionally when removing BankUpdate from the invoice wrapper.
- After cutover, BankUpdate must have exactly one active production scheduling path.
