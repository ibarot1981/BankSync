from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP
import re
from typing import Any

from .models import TransactionRecord

HEADER_ALIASES = {
    "Column 1": "Transaction Date",
    "Transaction Date": "Transaction Date",
    "Transaction Description": "Transaction Description",
    "Transaction Amount": "Transaction Amount",
    "Bank": "Bank",
    "Reference No.": "Reference No.",
    "Value Date": "Value Date",
    "Running Balance": "Running Balance",
}

CANONICAL_TRANSACTION_FIELDS = (
    "Bank",
    "Value Date",
    "Transaction Amount",
    "Reference No.",
    "Transaction Description",
    "Transaction Date",
)


@dataclass
class NormalizationResult:
    transaction: TransactionRecord
    normalized_payload: dict[str, Any]


def normalize_text(value: Any) -> str:
    if value is None:
        return ""
    text = str(value).strip()
    return re.sub(r"\s+", " ", text)


def normalize_amount(value: Any) -> str:
    text = normalize_text(value)
    if not text:
        return "0.00"
    cleaned = text.replace(",", "").replace("₹", "").replace("$", "")
    try:
        decimal_value = Decimal(cleaned).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)
    except InvalidOperation as exc:
        raise ValueError(f"Invalid amount value: {value!r}") from exc
    return format(decimal_value, "f")


def _parse_with_formats(value: str, formats: list[str]) -> datetime | None:
    for fmt in formats:
        try:
            return datetime.strptime(value, fmt)
        except ValueError:
            continue
    return None


def _normalize_ampm(text: str) -> str:
    normalized = text.replace("am", "AM").replace("pm", "PM")
    normalized = normalized.replace("Am", "AM").replace("Pm", "PM")
    return normalized


def _parse_timestamp_value(value: Any) -> datetime | None:
    if isinstance(value, (int, float)):
        return datetime.fromtimestamp(int(value))

    text = normalize_text(value)
    if text.isdigit():
        numeric = int(text)
        if 0 <= numeric <= 4102444800:
            return datetime.fromtimestamp(numeric)
    return None


def parse_transaction_date(value: Any, bank: str) -> str:
    timestamp_value = _parse_timestamp_value(value)
    if timestamp_value is not None:
        return timestamp_value.strftime("%Y-%m-%d %H:%M:%S")

    text = normalize_text(value)
    if not text:
        return ""
    text = text.rstrip(":")
    text = _normalize_ampm(text)

    day_first_formats = [
        "%d-%m-%Y %H:%M:%S",
        "%d-%m-%Y %H:%M",
        "%d-%m-%Y %I:%M:%S %p",
        "%d-%m-%Y %I:%M %p",
        "%d/%m/%Y %H:%M:%S",
        "%d/%m/%Y %H:%M",
        "%d/%m/%Y %I:%M:%S %p",
        "%d/%m/%Y %I:%M %p",
        "%d/%m/%y %H:%M:%S",
        "%d/%m/%y %H:%M",
        "%d/%m/%y %I:%M:%S %p",
        "%d/%m/%y %I:%M %p",
        "%d-%m-%Y",
        "%d/%m/%Y",
        "%d/%m/%y",
    ]
    month_first_formats = [
        "%m-%d-%Y %H:%M:%S",
        "%m-%d-%Y %H:%M",
        "%m-%d-%Y %I:%M:%S %p",
        "%m-%d-%Y %I:%M %p",
        "%m/%d/%Y %H:%M:%S",
        "%m/%d/%Y %H:%M",
        "%m/%d/%Y %I:%M:%S %p",
        "%m/%d/%Y %I:%M %p",
        "%m/%d/%y %H:%M:%S",
        "%m/%d/%y %H:%M",
        "%m/%d/%y %I:%M:%S %p",
        "%m/%d/%y %I:%M %p",
        "%m-%d-%Y",
        "%m/%d/%Y",
        "%m/%d/%y",
    ]
    fallback_formats = [
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%d",
    ]

    upper_bank = bank.upper()
    if upper_bank == "ICICI":
        formats = month_first_formats + day_first_formats + fallback_formats
    else:
        formats = day_first_formats + month_first_formats + fallback_formats

    parsed = _parse_with_formats(text, formats)
    if parsed is None:
        raise ValueError(f"Unable to parse transaction date {value!r} for bank {bank!r}")
    return parsed.strftime("%Y-%m-%d %H:%M:%S")


def parse_value_date(value: Any) -> str:
    timestamp_value = _parse_timestamp_value(value)
    if timestamp_value is not None:
        return timestamp_value.strftime("%Y-%m-%d")

    text = normalize_text(value)
    if not text:
        return ""
    text = _normalize_ampm(text)
    formats = [
        "%d-%m-%Y",
        "%d/%m/%Y",
        "%d/%m/%y",
        "%Y-%m-%d",
        "%d-%m-%Y %H:%M:%S",
        "%d/%m/%Y %H:%M:%S",
        "%d/%m/%y %H:%M:%S",
    ]
    parsed = _parse_with_formats(text, formats)
    if parsed is None:
        raise ValueError(f"Unable to parse value date {value!r}")
    return parsed.strftime("%Y-%m-%d")


def alias_sheet_record(raw_record: dict[str, Any]) -> dict[str, Any]:
    aliased: dict[str, Any] = {}
    extras: dict[str, Any] = {}
    for key, value in raw_record.items():
        canonical = HEADER_ALIASES.get(key)
        if canonical:
            aliased[canonical] = value
        elif key != "Row_Num":
            extras[key] = value
    aliased["_extras"] = extras
    return aliased


def normalize_sheet_record(raw_record: dict[str, Any], source_origin: str) -> NormalizationResult:
    aliased = alias_sheet_record(raw_record)
    bank = normalize_text(aliased.get("Bank"))
    row_num = raw_record.get("Row_Num")
    row_num_value = int(row_num) if row_num not in (None, "") else None

    transaction = TransactionRecord(
        bank=bank,
        value_date=parse_value_date(aliased.get("Value Date")),
        transaction_amount=normalize_amount(aliased.get("Transaction Amount")),
        reference_no=normalize_text(aliased.get("Reference No.")),
        transaction_description=normalize_text(aliased.get("Transaction Description")),
        transaction_date=parse_transaction_date(aliased.get("Transaction Date"), bank or "UNKNOWN"),
        source_row_num=row_num_value,
        source_origin=source_origin,
        grist_seeded=source_origin == "grist_seed",
        running_balance=normalize_text(aliased.get("Running Balance")),
        extras=aliased.get("_extras", {}),
    )
    return NormalizationResult(transaction=transaction, normalized_payload=transaction.to_payload())


def normalize_grist_record(raw_fields: dict[str, Any], source_origin: str) -> NormalizationResult:
    mapped = {
        "Bank": raw_fields.get("Bank"),
        "Value Date": raw_fields.get("Value_Date") or raw_fields.get("Value Date"),
        "Transaction Amount": raw_fields.get("Transaction_Amount") or raw_fields.get("Transaction Amount"),
        "Reference No.": raw_fields.get("Reference_No") or raw_fields.get("Reference No."),
        "Transaction Description": raw_fields.get("Transaction_Description") or raw_fields.get("Transaction Description"),
        "Transaction Date": raw_fields.get("Transaction_Date") or raw_fields.get("Transaction Date"),
        "Running Balance": raw_fields.get("Running_Balance") or raw_fields.get("Running Balance"),
        "Row_Num": raw_fields.get("GSheets_RowNum"),
    }
    extras = {
        key: value
        for key, value in raw_fields.items()
        if key
        not in {
            "Bank",
            "Value_Date",
            "Value Date",
            "Transaction_Amount",
            "Transaction Amount",
            "Reference_No",
            "Reference No.",
            "Transaction_Description",
            "Transaction Description",
            "Transaction_Date",
            "Transaction Date",
            "Running_Balance",
            "Running Balance",
            "GSheets_RowNum",
        }
    }
    mapped["_extras"] = extras
    result = normalize_sheet_record(mapped, source_origin=source_origin)
    result.transaction.grist_seeded = True
    result.transaction.extras.update(extras)
    return result
