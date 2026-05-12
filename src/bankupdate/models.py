from __future__ import annotations

from dataclasses import dataclass, field
from decimal import Decimal
from typing import Any


@dataclass
class TransactionRecord:
    bank: str
    value_date: str
    transaction_amount: str
    reference_no: str
    transaction_description: str
    transaction_date: str
    source_row_num: int | None
    source_origin: str
    grist_seeded: bool
    fingerprint: str = ""
    running_balance: str = ""
    extras: dict[str, Any] = field(default_factory=dict)

    @property
    def amount_decimal(self) -> Decimal:
        return Decimal(self.transaction_amount)

    def to_payload(self) -> dict[str, Any]:
        return {
            "Bank": self.bank,
            "Value Date": self.value_date,
            "Transaction Amount": self.transaction_amount,
            "Reference No.": self.reference_no,
            "Transaction Description": self.transaction_description,
            "Transaction Date": self.transaction_date,
            "Running Balance": self.running_balance,
            "GSheets_RowNum": self.source_row_num,
            **self.extras,
        }


@dataclass
class ManualReviewIssue:
    issue_type: str
    source_row_num: int | None
    fingerprint: str | None
    details: dict[str, Any]
