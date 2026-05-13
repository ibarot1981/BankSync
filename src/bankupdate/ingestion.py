from __future__ import annotations

from dataclasses import asdict
from datetime import datetime
import json
import logging
from pathlib import Path
import uuid

from .bootstrap import bootstrap_from_grist
from .config import AppConfig
from .delivery import deliver_pending_queue
from .fingerprint import compute_fingerprint
from .google_sheets_client import GoogleSheetsClient
from .grist_client import GristClient
from .locking import FileLock, LockAcquisitionError
from .models import ManualReviewIssue
from .normalization import normalize_sheet_record
from .sqlite_store import SQLiteStore


LOCK_EXIT_CODE = 10


def _new_run_id(prefix: str) -> str:
    return f"{prefix}-{datetime.now().strftime('%Y%m%d%H%M%S')}-{uuid.uuid4().hex[:8]}"


def _write_manual_review_report(
    config: AppConfig,
    run_id: str,
    issues: list[ManualReviewIssue],
) -> None:
    if not issues:
        return
    report_path = config.runtime.reports_dir / f"manual_review_{run_id}.jsonl"
    with report_path.open("w", encoding="utf-8") as handle:
        for issue in issues:
            handle.write(json.dumps(asdict(issue), ensure_ascii=True, sort_keys=True))
            handle.write("\n")


def _process_sheet_rows(
    store: SQLiteStore,
    rows: list[dict[str, object]],
    last_ingested_row: int | None,
    run_id: str,
) -> tuple[int, int, int, list[ManualReviewIssue]]:
    current_fingerprints_by_row: dict[int, str] = {}
    normalized_rows = []
    manual_reviews: list[ManualReviewIssue] = []
    fetched_count = len(rows)

    for row in rows:
        try:
            normalized = normalize_sheet_record(row, source_origin="sheet_delta")
        except Exception as exc:
            issue = ManualReviewIssue(
                issue_type="normalization_error",
                source_row_num=int(row["Row_Num"]) if row.get("Row_Num") not in (None, "") else None,
                fingerprint=None,
                details={"error": str(exc), "row": row},
            )
            created = store.create_manual_review(issue)
            if created:
                manual_reviews.append(issue)
                store.add_audit_event(
                    run_id,
                    "manual_review_created",
                    issue.details,
                    fingerprint=None,
                )
            continue

        transaction = normalized.transaction
        transaction.fingerprint = compute_fingerprint(transaction)
        if transaction.source_row_num is not None:
            current_fingerprints_by_row[transaction.source_row_num] = transaction.fingerprint
        normalized_rows.append(normalized)

    new_count = 0
    queued_count = 0

    for normalized in normalized_rows:
        transaction = normalized.transaction
        row_num = transaction.source_row_num
        if row_num is None:
            continue

        prior_state = store.get_row_state(row_num)
        if prior_state and prior_state["fingerprint"] != transaction.fingerprint:
            prior_fingerprint = prior_state["fingerprint"]
            shifted = prior_fingerprint in current_fingerprints_by_row.values()
            if not shifted:
                issue = ManualReviewIssue(
                    issue_type="historical_row_changed",
                    source_row_num=row_num,
                    fingerprint=transaction.fingerprint,
                    details={
                        "prior_fingerprint": prior_fingerprint,
                        "current_fingerprint": transaction.fingerprint,
                        "row_num": row_num,
                    },
                )
                created = store.create_manual_review(issue)
                if created:
                    manual_reviews.append(issue)
                    store.add_audit_event(
                        run_id,
                        "manual_review_created",
                        issue.details,
                        fingerprint=transaction.fingerprint,
                    )
                store.set_row_state(row_num, transaction.fingerprint, transaction.to_payload())
                continue

        inserted = False
        if not store.transaction_exists(transaction.fingerprint):
            inserted = store.insert_transaction(transaction)
            if inserted:
                new_count += 1
                store.add_audit_event(
                    run_id,
                    "transaction_inserted",
                    {"row_num": row_num, "source_origin": transaction.source_origin},
                    fingerprint=transaction.fingerprint,
                )

        if inserted:
            queued = store.mark_queue_pending(transaction.fingerprint)
            if queued:
                queued_count += 1
                store.add_audit_event(
                    run_id,
                    "queue_pending",
                    {"row_num": row_num},
                    fingerprint=transaction.fingerprint,
                )

        store.set_row_state(row_num, transaction.fingerprint, transaction.to_payload())

    if rows and last_ingested_row is not None:
        highest_row = max(int(row["Row_Num"]) for row in rows)
        store.set_last_ingested_row(max(last_ingested_row, highest_row))
    elif rows:
        store.set_last_ingested_row(max(int(row["Row_Num"]) for row in rows))

    return fetched_count, new_count, queued_count, manual_reviews


def run_daily(config: AppConfig, logger: logging.Logger, force: bool = False) -> int:
    today_key = datetime.now().strftime("%Y-%m-%d")
    lock = FileLock(config.runtime.lock_path, config.lock_stale_after_seconds)
    store = SQLiteStore(config.runtime.db_path)
    store.initialize()
    run_id = _new_run_id("daily")
    logger.info("Starting daily run %s (force=%s).", run_id, force)

    try:
        lock.acquire()
    except LockAcquisitionError as exc:
        logger.warning("Another BankUpdate run is already active. Exiting daily mode. %s", exc)
        store.close()
        return LOCK_EXIT_CODE

    grist_client = GristClient(config)
    sheets_client = GoogleSheetsClient(config)

    fetched_count = 0
    new_count = 0
    queued_count = 0
    delivered_count = 0
    retry_count = 0
    manual_reviews: list[ManualReviewIssue] = []

    try:
        with store.transaction():
            store.create_run(run_id, "daily")

        if config.skip_daily_if_already_successful and not force and store.get_last_daily_success_date() == today_key:
            logger.info("Daily run already completed successfully today. Skipping duplicate daily execution.")
            with store.transaction():
                store.complete_run(run_id, "skipped")
            return 0

        if not store.has_transactions():
            logger.info("SQLite is empty; starting bootstrap from Grist.")
            with store.transaction():
                bootstrap_summary = bootstrap_from_grist(store, grist_client, sheets_client, logger, run_id)
            logger.info(
                "Bootstrap completed: seeded=%s enriched=%s",
                bootstrap_summary["seeded_count"],
                bootstrap_summary["enriched_count"],
            )

        last_ingested_row = store.get_last_ingested_row() or 1
        start_row = max(2, last_ingested_row - config.replay_window_rows + 1)
        rows = sheets_client.fetch_rows_from(start_row)
        logger.info(
            "Fetched %s Google Sheets rows from row %s onward (last_ingested_row=%s).",
            len(rows),
            start_row,
            last_ingested_row,
        )

        with store.transaction():
            fetched_count, new_count, queued_count, manual_reviews = _process_sheet_rows(
                store,
                rows,
                last_ingested_row,
                run_id,
            )

        with store.transaction():
            delivered_count, retry_count = deliver_pending_queue(store, config, grist_client, logger, run_id)
            store.complete_run(
                run_id,
                "success",
                fetched_count=fetched_count,
                new_count=new_count,
                queued_count=queued_count,
                delivered_count=delivered_count,
                retry_count=retry_count,
                manual_review_count=len(manual_reviews),
            )
            store.set_last_daily_success_date(today_key)
            store.set_state("last_daily_run_at", datetime.now().isoformat(timespec="seconds"))

        _write_manual_review_report(config, run_id, manual_reviews)
        logger.info(
            "Daily run %s completed: fetched=%s new=%s queued=%s delivered=%s retry=%s manual_review=%s.",
            run_id,
            fetched_count,
            new_count,
            queued_count,
            delivered_count,
            retry_count,
            len(manual_reviews),
        )
        return 0
    except Exception as exc:
        with store.transaction():
            store.complete_run(
                run_id,
                "failed",
                fetched_count=fetched_count,
                new_count=new_count,
                queued_count=queued_count,
                delivered_count=delivered_count,
                retry_count=retry_count,
                manual_review_count=len(manual_reviews),
                error_summary=str(exc),
            )
        logger.exception("Daily run failed.")
        return 1
    finally:
        lock.release()
        store.close()


def run_retry(config: AppConfig, logger: logging.Logger) -> int:
    lock = FileLock(config.runtime.lock_path, config.lock_stale_after_seconds)
    store = SQLiteStore(config.runtime.db_path)
    store.initialize()
    run_id = _new_run_id("retry")
    logger.info("Starting retry run %s.", run_id)

    try:
        lock.acquire()
    except LockAcquisitionError as exc:
        logger.warning("Another BankUpdate run is already active. Exiting retry mode. %s", exc)
        store.close()
        return LOCK_EXIT_CODE

    grist_client = GristClient(config)

    try:
        with store.transaction():
            store.create_run(run_id, "retry")
            delivered_count, retry_count = deliver_pending_queue(store, config, grist_client, logger, run_id)
            store.complete_run(
                run_id,
                "success",
                delivered_count=delivered_count,
                retry_count=retry_count,
            )
            store.set_state("last_retry_run_at", datetime.now().isoformat(timespec="seconds"))
        logger.info(
            "Retry run %s completed: delivered=%s left_for_retry=%s.",
            run_id,
            delivered_count,
            retry_count,
        )
        return 0
    except Exception as exc:
        with store.transaction():
            store.complete_run(run_id, "failed", error_summary=str(exc))
        logger.exception("Retry run failed.")
        return 1
    finally:
        lock.release()
        store.close()


def run_bootstrap(config: AppConfig, logger: logging.Logger) -> int:
    lock = FileLock(config.runtime.lock_path, config.lock_stale_after_seconds)
    store = SQLiteStore(config.runtime.db_path)
    store.initialize()
    run_id = _new_run_id("bootstrap")
    logger.info("Starting bootstrap run %s.", run_id)

    try:
        lock.acquire()
    except LockAcquisitionError as exc:
        logger.warning("Another BankUpdate run is already active. Exiting bootstrap mode. %s", exc)
        store.close()
        return LOCK_EXIT_CODE

    grist_client = GristClient(config)
    sheets_client = GoogleSheetsClient(config)

    try:
        with store.transaction():
            store.create_run(run_id, "bootstrap")
            summary = bootstrap_from_grist(store, grist_client, sheets_client, logger, run_id)
            store.complete_run(
                run_id,
                "success",
                new_count=summary["seeded_count"],
            )
        logger.info("Bootstrap summary: %s", summary)
        return 0
    except Exception as exc:
        with store.transaction():
            store.complete_run(run_id, "failed", error_summary=str(exc))
        logger.exception("Bootstrap failed.")
        return 1
    finally:
        lock.release()
        store.close()


def run_health(config: AppConfig, logger: logging.Logger) -> int:
    store = SQLiteStore(config.runtime.db_path)
    store.initialize()
    logger.info("Starting health check.")

    try:
        sheets_client = GoogleSheetsClient(config)
        grist_client = GristClient(config)
        sheet_health = sheets_client.healthcheck()
        grist_health = grist_client.healthcheck()
        logger.info("Google Sheets health: %s", sheet_health)
        logger.info("Grist health: %s", grist_health)
        logger.info("SQLite DB path: %s", config.runtime.db_path)
        return 0
    except Exception:
        logger.exception("Health check failed.")
        return 1
    finally:
        store.close()
