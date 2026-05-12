from __future__ import annotations

import argparse
from pathlib import Path

from .config import load_config
from .ingestion import run_bootstrap, run_daily, run_health, run_retry
from .logging_utils import setup_logging


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="BankUpdate Phase 1 CLI")
    subparsers = parser.add_subparsers(dest="mode", required=True)

    daily_parser = subparsers.add_parser("daily", help="Run daily ingestion and delivery flow")
    daily_parser.add_argument("--force", action="store_true", help="Run even if a successful daily run already happened today")

    subparsers.add_parser("retry", help="Retry pending Grist deliveries only")
    subparsers.add_parser("bootstrap", help="Initialize SQLite from Grist and sheet enrichment")
    subparsers.add_parser("health", help="Verify Google Sheets, Grist, and SQLite connectivity")
    return parser


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()

    repo_root = Path(__file__).resolve().parents[2]
    config = load_config(repo_root)
    logger = setup_logging(config)

    if args.mode == "daily":
        return run_daily(config, logger, force=args.force)
    if args.mode == "retry":
        return run_retry(config, logger)
    if args.mode == "bootstrap":
        return run_bootstrap(config, logger)
    if args.mode == "health":
        return run_health(config, logger)

    parser.error(f"Unsupported mode: {args.mode}")
    return 2


if __name__ == "__main__":
    raise SystemExit(main())
