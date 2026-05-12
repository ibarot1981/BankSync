from __future__ import annotations

import json
import sqlite3
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable

from .models import ManualReviewIssue, TransactionRecord


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


class SQLiteStore:
    def __init__(self, db_path: Path):
        self.db_path = db_path
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self.connection = sqlite3.connect(self.db_path)
        self.connection.row_factory = sqlite3.Row
        self.connection.execute("PRAGMA journal_mode=WAL")
        self.connection.execute("PRAGMA foreign_keys=ON")

    def close(self) -> None:
        self.connection.close()

    @contextmanager
    def transaction(self) -> Iterable[sqlite3.Connection]:
        try:
            yield self.connection
            self.connection.commit()
        except Exception:
            self.connection.rollback()
            raise

    def initialize(self) -> None:
        with self.transaction() as conn:
            conn.executescript(
                """
                CREATE TABLE IF NOT EXISTS app_state (
                    key TEXT PRIMARY KEY,
                    value TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS transactions (
                    fingerprint TEXT PRIMARY KEY,
                    bank TEXT NOT NULL,
                    value_date TEXT NOT NULL,
                    transaction_date TEXT NOT NULL,
                    transaction_amount TEXT NOT NULL,
                    reference_no TEXT NOT NULL,
                    transaction_description TEXT NOT NULL,
                    source_row_num INTEGER,
                    source_snapshot_date TEXT,
                    source_origin TEXT NOT NULL,
                    grist_seeded INTEGER NOT NULL,
                    created_at TEXT NOT NULL,
                    normalized_payload_json TEXT NOT NULL
                );

                CREATE INDEX IF NOT EXISTS idx_transactions_source_row_num
                ON transactions(source_row_num);

                CREATE TABLE IF NOT EXISTS delivery_queue (
                    fingerprint TEXT PRIMARY KEY,
                    status TEXT NOT NULL,
                    attempt_count INTEGER NOT NULL DEFAULT 0,
                    first_queued_at TEXT NOT NULL,
                    last_attempt_at TEXT,
                    last_error TEXT,
                    delivered_at TEXT,
                    grist_record_id TEXT
                );

                CREATE INDEX IF NOT EXISTS idx_delivery_queue_status
                ON delivery_queue(status);

                CREATE TABLE IF NOT EXISTS audit_events (
                    event_id INTEGER PRIMARY KEY AUTOINCREMENT,
                    run_id TEXT NOT NULL,
                    fingerprint TEXT,
                    event_type TEXT NOT NULL,
                    event_time TEXT NOT NULL,
                    details_json TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS run_history (
                    run_id TEXT PRIMARY KEY,
                    run_type TEXT NOT NULL,
                    started_at TEXT NOT NULL,
                    completed_at TEXT,
                    status TEXT NOT NULL,
                    fetched_count INTEGER NOT NULL DEFAULT 0,
                    new_count INTEGER NOT NULL DEFAULT 0,
                    queued_count INTEGER NOT NULL DEFAULT 0,
                    delivered_count INTEGER NOT NULL DEFAULT 0,
                    retry_count INTEGER NOT NULL DEFAULT 0,
                    manual_review_count INTEGER NOT NULL DEFAULT 0,
                    error_summary TEXT
                );

                CREATE TABLE IF NOT EXISTS manual_review (
                    review_id INTEGER PRIMARY KEY AUTOINCREMENT,
                    fingerprint TEXT,
                    source_row_num INTEGER,
                    issue_type TEXT NOT NULL,
                    detected_at TEXT NOT NULL,
                    status TEXT NOT NULL,
                    details_json TEXT NOT NULL
                );

                CREATE INDEX IF NOT EXISTS idx_manual_review_open
                ON manual_review(status, source_row_num, issue_type);

                CREATE TABLE IF NOT EXISTS sheet_row_state (
                    row_num INTEGER PRIMARY KEY,
                    fingerprint TEXT NOT NULL,
                    last_seen_at TEXT NOT NULL,
                    payload_json TEXT NOT NULL
                );
                """
            )
            self.set_state("schema_version", "1")

    def get_state(self, key: str, default: str | None = None) -> str | None:
        row = self.connection.execute(
            "SELECT value FROM app_state WHERE key = ?",
            (key,),
        ).fetchone()
        return row["value"] if row else default

    def set_state(self, key: str, value: str) -> None:
        now = utc_now_iso()
        self.connection.execute(
            """
            INSERT INTO app_state(key, value, updated_at)
            VALUES(?, ?, ?)
            ON CONFLICT(key) DO UPDATE SET
                value = excluded.value,
                updated_at = excluded.updated_at
            """,
            (key, value, now),
        )

    def has_transactions(self) -> bool:
        row = self.connection.execute("SELECT 1 FROM transactions LIMIT 1").fetchone()
        return row is not None

    def get_last_ingested_row(self) -> int | None:
        value = self.get_state("last_ingested_row")
        return int(value) if value else None

    def set_last_ingested_row(self, row_num: int) -> None:
        self.set_state("last_ingested_row", str(row_num))

    def get_last_daily_success_date(self) -> str | None:
        return self.get_state("last_daily_success_date")

    def set_last_daily_success_date(self, date_value: str) -> None:
        self.set_state("last_daily_success_date", date_value)

    def insert_transaction(self, record: TransactionRecord) -> bool:
        payload_json = json.dumps(record.to_payload(), ensure_ascii=True, sort_keys=True)
        cursor = self.connection.execute(
            """
            INSERT OR IGNORE INTO transactions(
                fingerprint, bank, value_date, transaction_date, transaction_amount,
                reference_no, transaction_description, source_row_num, source_snapshot_date,
                source_origin, grist_seeded, created_at, normalized_payload_json
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                record.fingerprint,
                record.bank,
                record.value_date,
                record.transaction_date,
                record.transaction_amount,
                record.reference_no,
                record.transaction_description,
                record.source_row_num,
                utc_now_iso()[:10],
                record.source_origin,
                1 if record.grist_seeded else 0,
                utc_now_iso(),
                payload_json,
            ),
        )
        return cursor.rowcount > 0

    def transaction_exists(self, fingerprint: str) -> bool:
        row = self.connection.execute(
            "SELECT 1 FROM transactions WHERE fingerprint = ?",
            (fingerprint,),
        ).fetchone()
        return row is not None

    def get_transaction(self, fingerprint: str) -> sqlite3.Row | None:
        return self.connection.execute(
            "SELECT * FROM transactions WHERE fingerprint = ?",
            (fingerprint,),
        ).fetchone()

    def mark_queue_pending(self, fingerprint: str) -> bool:
        cursor = self.connection.execute(
            """
            INSERT OR IGNORE INTO delivery_queue(
                fingerprint, status, attempt_count, first_queued_at
            )
            VALUES (?, 'pending', 0, ?)
            """,
            (fingerprint, utc_now_iso()),
        )
        return cursor.rowcount > 0

    def mark_queue_delivered(self, fingerprint: str, grist_record_id: str | None = None) -> None:
        self.connection.execute(
            """
            INSERT INTO delivery_queue(
                fingerprint, status, attempt_count, first_queued_at, delivered_at, grist_record_id
            )
            VALUES (?, 'delivered', 0, ?, ?, ?)
            ON CONFLICT(fingerprint) DO UPDATE SET
                status = 'delivered',
                delivered_at = excluded.delivered_at,
                grist_record_id = excluded.grist_record_id,
                last_error = NULL
            """,
            (fingerprint, utc_now_iso(), utc_now_iso(), grist_record_id),
        )

    def mark_queue_retry(self, fingerprint: str, error_text: str) -> None:
        self.connection.execute(
            """
            UPDATE delivery_queue
            SET status = 'retry',
                attempt_count = attempt_count + 1,
                last_attempt_at = ?,
                last_error = ?
            WHERE fingerprint = ?
            """,
            (utc_now_iso(), error_text, fingerprint),
        )

    def mark_queue_attempt_success(self, fingerprint: str, grist_record_id: str | None = None) -> None:
        self.connection.execute(
            """
            UPDATE delivery_queue
            SET status = 'delivered',
                attempt_count = attempt_count + 1,
                last_attempt_at = ?,
                delivered_at = ?,
                grist_record_id = ?,
                last_error = NULL
            WHERE fingerprint = ?
            """,
            (utc_now_iso(), utc_now_iso(), grist_record_id, fingerprint),
        )

    def get_queue_records(self, statuses: tuple[str, ...], limit: int | None = None) -> list[sqlite3.Row]:
        placeholders = ",".join("?" for _ in statuses)
        sql = f"""
            SELECT q.*, t.normalized_payload_json
            FROM delivery_queue q
            JOIN transactions t ON t.fingerprint = q.fingerprint
            WHERE q.status IN ({placeholders})
            ORDER BY q.first_queued_at ASC
        """
        params: list[Any] = list(statuses)
        if limit is not None:
            sql += " LIMIT ?"
            params.append(limit)
        return list(self.connection.execute(sql, params).fetchall())

    def create_run(self, run_id: str, run_type: str) -> None:
        self.connection.execute(
            """
            INSERT INTO run_history(run_id, run_type, started_at, status)
            VALUES (?, ?, ?, 'running')
            """,
            (run_id, run_type, utc_now_iso()),
        )

    def complete_run(
        self,
        run_id: str,
        status: str,
        fetched_count: int = 0,
        new_count: int = 0,
        queued_count: int = 0,
        delivered_count: int = 0,
        retry_count: int = 0,
        manual_review_count: int = 0,
        error_summary: str | None = None,
    ) -> None:
        self.connection.execute(
            """
            UPDATE run_history
            SET completed_at = ?,
                status = ?,
                fetched_count = ?,
                new_count = ?,
                queued_count = ?,
                delivered_count = ?,
                retry_count = ?,
                manual_review_count = ?,
                error_summary = ?
            WHERE run_id = ?
            """,
            (
                utc_now_iso(),
                status,
                fetched_count,
                new_count,
                queued_count,
                delivered_count,
                retry_count,
                manual_review_count,
                error_summary,
                run_id,
            ),
        )

    def add_audit_event(self, run_id: str, event_type: str, details: dict[str, Any], fingerprint: str | None = None) -> None:
        self.connection.execute(
            """
            INSERT INTO audit_events(run_id, fingerprint, event_type, event_time, details_json)
            VALUES (?, ?, ?, ?, ?)
            """,
            (run_id, fingerprint, event_type, utc_now_iso(), json.dumps(details, ensure_ascii=True, sort_keys=True)),
        )

    def create_manual_review(self, issue: ManualReviewIssue) -> bool:
        existing = self.connection.execute(
            """
            SELECT review_id
            FROM manual_review
            WHERE status = 'open'
              AND source_row_num IS ?
              AND issue_type = ?
            ORDER BY review_id DESC
            LIMIT 1
            """,
            (issue.source_row_num, issue.issue_type),
        ).fetchone()
        if existing:
            return False

        self.connection.execute(
            """
            INSERT INTO manual_review(fingerprint, source_row_num, issue_type, detected_at, status, details_json)
            VALUES (?, ?, ?, ?, 'open', ?)
            """,
            (
                issue.fingerprint,
                issue.source_row_num,
                issue.issue_type,
                utc_now_iso(),
                json.dumps(issue.details, ensure_ascii=True, sort_keys=True),
            ),
        )
        return True

    def get_row_state(self, row_num: int) -> sqlite3.Row | None:
        return self.connection.execute(
            "SELECT * FROM sheet_row_state WHERE row_num = ?",
            (row_num,),
        ).fetchone()

    def set_row_state(self, row_num: int, fingerprint: str, payload: dict[str, Any]) -> None:
        self.connection.execute(
            """
            INSERT INTO sheet_row_state(row_num, fingerprint, last_seen_at, payload_json)
            VALUES (?, ?, ?, ?)
            ON CONFLICT(row_num) DO UPDATE SET
                fingerprint = excluded.fingerprint,
                last_seen_at = excluded.last_seen_at,
                payload_json = excluded.payload_json
            """,
            (row_num, fingerprint, utc_now_iso(), json.dumps(payload, ensure_ascii=True, sort_keys=True)),
        )

    def get_max_seeded_row_num(self) -> int | None:
        row = self.connection.execute(
            "SELECT MAX(source_row_num) AS max_row FROM transactions",
        ).fetchone()
        value = row["max_row"] if row else None
        return int(value) if value is not None else None
