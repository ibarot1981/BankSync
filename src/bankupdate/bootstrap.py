from __future__ import annotations

import logging

from .fingerprint import compute_fingerprint
from .google_sheets_client import GoogleSheetsClient
from .grist_client import GristClient
from .models import TransactionRecord
from .normalization import normalize_grist_record, normalize_sheet_record
from .sqlite_store import SQLiteStore, utc_now_iso


def _needs_enrichment(record: TransactionRecord) -> bool:
    return any(
        [
            not record.transaction_date,
            not record.value_date,
            not record.bank,
            not record.transaction_amount,
            not record.transaction_description,
            not record.reference_no,
        ]
    )


def bootstrap_from_grist(
    store: SQLiteStore,
    grist_client: GristClient,
    sheets_client: GoogleSheetsClient,
    logger: logging.Logger,
    run_id: str,
) -> dict[str, int]:
    if store.has_transactions():
        return {"seeded_count": 0, "enriched_count": 0}

    grist_rows = grist_client.fetch_all_records()
    logger.info("Bootstrap seeding %s existing Grist records into SQLite.", len(grist_rows))

    all_sheet_rows = {int(row["Row_Num"]): row for row in sheets_client.get_all_rows()}
    seeded_count = 0
    enriched_count = 0
    max_row_num = 1

    for grist_row in grist_rows:
        fields = grist_row.get("fields", {})
        normalized = normalize_grist_record(fields, source_origin="grist_seed")
        transaction = normalized.transaction

        if transaction.source_row_num:
            max_row_num = max(max_row_num, int(transaction.source_row_num))

        if _needs_enrichment(transaction) and transaction.source_row_num in all_sheet_rows:
            normalized = normalize_sheet_record(
                all_sheet_rows[int(transaction.source_row_num)],
                source_origin="sheet_enrichment",
            )
            transaction = normalized.transaction
            transaction.grist_seeded = True
            enriched_count += 1

        transaction.fingerprint = compute_fingerprint(transaction)
        inserted = store.insert_transaction(transaction)
        if not inserted:
            continue

        seeded_count += 1
        store.mark_queue_delivered(transaction.fingerprint, str(grist_row.get("id")))
        if transaction.source_row_num is not None:
            store.set_row_state(transaction.source_row_num, transaction.fingerprint, transaction.to_payload())
        store.add_audit_event(
            run_id,
            "bootstrap_seeded",
            {
                "row_num": transaction.source_row_num,
                "source_origin": transaction.source_origin,
            },
            fingerprint=transaction.fingerprint,
        )

    store.set_last_ingested_row(max_row_num)
    store.set_state("cutover_row", str(max_row_num))
    store.set_state("last_bootstrap_at", utc_now_iso())
    return {"seeded_count": seeded_count, "enriched_count": enriched_count}
