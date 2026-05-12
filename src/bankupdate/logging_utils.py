from __future__ import annotations

import logging
from logging.handlers import RotatingFileHandler
import sys

from .config import AppConfig


def setup_logging(config: AppConfig) -> logging.Logger:
    logger = logging.getLogger("bankupdate")
    logger.setLevel(getattr(logging, config.log_level, logging.INFO))
    logger.propagate = False

    if logger.handlers:
        return logger

    formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")

    file_handler = RotatingFileHandler(
        config.runtime.log_path,
        maxBytes=config.log_max_bytes,
        backupCount=config.log_backup_count,
        encoding="utf-8",
    )
    file_handler.setFormatter(formatter)

    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(formatter)

    logger.addHandler(file_handler)
    logger.addHandler(console_handler)
    return logger
