from __future__ import annotations

import hashlib

from .models import TransactionRecord


def compute_fingerprint(record: TransactionRecord) -> str:
    canonical = "|".join(
        [
            record.bank,
            record.value_date,
            record.transaction_amount,
            record.reference_no,
            record.transaction_description,
            record.transaction_date,
        ]
    )
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()
