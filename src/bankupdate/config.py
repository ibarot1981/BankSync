from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

from dotenv import load_dotenv

from .paths import RuntimePaths, build_runtime_paths


def _as_bool(value: str | None, default: bool) -> bool:
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "on"}


@dataclass(frozen=True)
class AppConfig:
    repo_root: Path
    runtime: RuntimePaths
    gsheet_credentials_path: Path
    gsheet_id: str
    worksheet_name: str
    grist_base_host: str
    grist_api_key: str
    grist_doc_id: str
    grist_table_name: str
    log_level: str
    log_max_bytes: int
    log_backup_count: int
    replay_window_rows: int
    delivery_batch_size: int
    grist_request_timeout_seconds: int
    lock_stale_after_seconds: int
    skip_daily_if_already_successful: bool


def load_config(repo_root: Path | None = None) -> AppConfig:
    resolved_root = repo_root or Path(__file__).resolve().parents[2]
    load_dotenv(resolved_root / ".env")

    runtime = build_runtime_paths(
        resolved_root,
        os.getenv("BANKUPDATE_RUNTIME_DIR", "runtime"),
    )

    gsheet_credentials_value = os.getenv("GSHEET_CREDENTIALS_PATH", "./service-account-credentials.json")
    gsheet_credentials_path = Path(gsheet_credentials_value)
    if not gsheet_credentials_path.is_absolute():
        gsheet_credentials_path = (resolved_root / gsheet_credentials_path).resolve()

    required = {
        "GSHEET_ID": os.getenv("GSHEET_ID"),
        "GRIST_API_KEY": os.getenv("GRIST_API_KEY"),
        "GRIST_DOC_ID": os.getenv("GRIST_DOC_ID"),
        "GRIST_TABLE_NAME": os.getenv("GRIST_TABLE_NAME"),
    }
    missing = [name for name, value in required.items() if not value]
    if missing:
        raise ValueError(f"Missing required environment variables: {', '.join(missing)}")

    return AppConfig(
        repo_root=resolved_root,
        runtime=runtime,
        gsheet_credentials_path=gsheet_credentials_path,
        gsheet_id=required["GSHEET_ID"] or "",
        worksheet_name=os.getenv("WORKSHEET_NAME", "Payment Receipt").strip('"'),
        grist_base_host=os.getenv("GRIST_BASE_HOST", "http://safcost.duckdns.org:8484").rstrip("/"),
        grist_api_key=required["GRIST_API_KEY"] or "",
        grist_doc_id=required["GRIST_DOC_ID"] or "",
        grist_table_name=required["GRIST_TABLE_NAME"] or "",
        log_level=os.getenv("LOG_LEVEL", "INFO").upper(),
        log_max_bytes=int(os.getenv("LOG_MAX_BYTES", str(5 * 1024 * 1024))),
        log_backup_count=int(os.getenv("LOG_BACKUP_COUNT", "5")),
        replay_window_rows=int(os.getenv("BANKUPDATE_REPLAY_WINDOW_ROWS", "200")),
        delivery_batch_size=int(os.getenv("BANKUPDATE_DELIVERY_BATCH_SIZE", "100")),
        grist_request_timeout_seconds=int(os.getenv("BANKUPDATE_GRIST_TIMEOUT_SECONDS", "30")),
        lock_stale_after_seconds=int(os.getenv("BANKUPDATE_LOCK_STALE_AFTER_SECONDS", str(4 * 60 * 60))),
        skip_daily_if_already_successful=_as_bool(
            os.getenv("BANKUPDATE_SKIP_DAILY_IF_ALREADY_SUCCESSFUL"),
            default=True,
        ),
    )
