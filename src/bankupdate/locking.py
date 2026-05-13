from __future__ import annotations

import json
import os
import time
from dataclasses import dataclass
from pathlib import Path


class LockAcquisitionError(RuntimeError):
    """Raised when another BankUpdate run still owns the lock."""


@dataclass
class FileLock:
    path: Path
    stale_after_seconds: int

    def acquire(self) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        now = time.time()

        if self.path.exists():
            try:
                payload = json.loads(self.path.read_text(encoding="utf-8"))
                locked_at = float(payload.get("locked_at", 0))
                owner_pid = payload.get("pid")
            except Exception:
                locked_at = 0
                owner_pid = None

            if now - locked_at <= self.stale_after_seconds:
                age_seconds = round(now - locked_at, 2)
                raise LockAcquisitionError(
                    f"Lock already held: {self.path} "
                    f"(owner_pid={owner_pid}, age_seconds={age_seconds}, stale_after_seconds={self.stale_after_seconds})"
                )
            self.path.unlink(missing_ok=True)

        payload = {"pid": os.getpid(), "locked_at": now}
        with self.path.open("x", encoding="utf-8") as handle:
            json.dump(payload, handle)

    def release(self) -> None:
        self.path.unlink(missing_ok=True)
