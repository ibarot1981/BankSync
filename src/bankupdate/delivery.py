from __future__ import annotations

import json
import logging
from typing import Any

from .config import AppConfig
from .grist_client import GristClient
from .models import TransactionRecord
from .sqlite_store import SQLiteStore


def _row_to_transaction(row: dict[str, Any]) -> TransactionRecord:
    payload = json.loads(row["normalized_payload_json"])
    return TransactionRecord(
        bank=payload.get("Bank", ""),
        value_date=payload.get("Value Date", ""),
        transaction_amount=payload.get("Transaction Amount", "0.00"),
        reference_no=payload.get("Reference No.", ""),
        transaction_description=payload.get("Transaction Description", ""),
        transaction_date=payload.get("Transaction Date", ""),
        source_row_num=payload.get("GSheets_RowNum"),
        source_origin="delivery_queue",
        grist_seeded=False,
        fingerprint=row["fingerprint"],
        running_balance=payload.get("Running Balance", ""),
        extras={
            key: value
            for key, value in payload.items()
            if key
            not in {
                "Bank",
                "Value Date",
                "Transaction Amount",
                "Reference No.",
                "Transaction Description",
                "Transaction Date",
                "Running Balance",
                "GSheets_RowNum",
            }
        },
    )


def deliver_pending_queue(
    store: SQLiteStore,
    config: AppConfig,
    grist_client: GristClient,
    logger: logging.Logger,
    run_id: str,
) -> tuple[int, int]:
    rows = store.get_queue_records(("pending", "retry"))
    if not rows:
        return 0, 0

    if not grist_client.is_available():
        logger.warning("Grist unavailable; leaving queue items pending for retry.")
        return 0, len(rows)

    columns = grist_client.get_columns()
    delivered_count = 0
    retry_count = 0

    for batch_start in range(0, len(rows), config.delivery_batch_size):
        batch = rows[batch_start : batch_start + config.delivery_batch_size]
        transactions = [_row_to_transaction(dict(row)) for row in batch]
        payloads = [grist_client.map_transaction_to_grist_fields(record, columns) for record in transactions]

        try:
            created_ids = grist_client.create_records(payloads)
        except Exception as exc:
            error_text = str(exc)
            for row in batch:
                store.mark_queue_retry(row["fingerprint"], error_text)
                retry_count += 1
                store.add_audit_event(
                    run_id,
                    "delivery_retry",
                    {"error": error_text},
                    fingerprint=row["fingerprint"],
                )
            continue

        for index, row in enumerate(batch):
            grist_record_id = created_ids[index] if index < len(created_ids) else None
            store.mark_queue_attempt_success(row["fingerprint"], grist_record_id)
            delivered_count += 1
            store.add_audit_event(
                run_id,
                "delivery_success",
                {"grist_record_id": grist_record_id},
                fingerprint=row["fingerprint"],
            )

    return delivered_count, retry_count
