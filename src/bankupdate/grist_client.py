from __future__ import annotations

import json
from typing import Any

import requests

from .config import AppConfig
from .models import TransactionRecord


class GristClient:
    def __init__(self, config: AppConfig):
        self.base_host = config.grist_base_host
        self.doc_id = config.grist_doc_id
        self.table_name = config.grist_table_name
        self.timeout = config.grist_request_timeout_seconds
        self.session = requests.Session()
        self.session.headers.update(
            {
                "Authorization": f"Bearer {config.grist_api_key}",
                "Content-Type": "application/json",
            }
        )

    @property
    def records_url(self) -> str:
        return f"{self.base_host}/api/docs/{self.doc_id}/tables/{self.table_name}/records"

    @property
    def columns_url(self) -> str:
        return f"{self.base_host}/api/docs/{self.doc_id}/tables/{self.table_name}/columns"

    def _request(self, method: str, url: str, **kwargs: Any) -> requests.Response:
        response = self.session.request(method, url, timeout=self.timeout, **kwargs)
        response.raise_for_status()
        return response

    def is_available(self) -> bool:
        try:
            response = self.session.get(self.base_host, timeout=5)
            return response.status_code == 200
        except requests.RequestException:
            return False

    def get_columns(self) -> list[dict[str, Any]]:
        response = self._request("GET", self.columns_url)
        return response.json().get("columns", [])

    def healthcheck(self) -> dict[str, Any]:
        response = self._request("GET", f"{self.base_host}/api/docs/{self.doc_id}")
        columns = self.get_columns()
        return {
            "doc_id": self.doc_id,
            "table_name": self.table_name,
            "document_name": response.json().get("name"),
            "column_count": len(columns),
        }

    def fetch_all_records(self, page_size: int = 5000) -> list[dict[str, Any]]:
        records: list[dict[str, Any]] = []
        offset = 0
        while True:
            response = self._request(
                "GET",
                self.records_url,
                params={"limit": page_size, "offset": offset},
            )
            chunk = response.json().get("records", [])
            records.extend(chunk)
            if len(chunk) < page_size:
                break
            offset += page_size
        return records

    def create_records(self, payloads: list[dict[str, Any]]) -> list[str]:
        if not payloads:
            return []
        response = self.session.post(
            self.records_url,
            timeout=self.timeout,
            data=json.dumps({"records": [{"fields": payload} for payload in payloads]}),
        )
        try:
            response.raise_for_status()
        except requests.HTTPError as exc:
            body = response.text[:2000]
            raise requests.HTTPError(f"{exc} | response body: {body}") from exc
        created = response.json().get("records", [])
        return [str(item.get("id")) for item in created]

    def map_transaction_to_grist_fields(
        self,
        record: TransactionRecord,
        columns: list[dict[str, Any]],
    ) -> dict[str, Any]:
        label_to_id = {column.get("label"): column.get("id") for column in columns}
        id_set = {column.get("id") for column in columns}
        fields: dict[str, Any] = {}

        base_field_map = {
            "Bank": record.bank,
            "Transaction_Date": record.transaction_date,
            "Transaction_Amount": float(record.transaction_amount),
            "Transaction_Description": record.transaction_description,
            "Reference_No": record.reference_no,
            "Value_Date": record.value_date,
        }
        if record.source_row_num is not None:
            base_field_map["GSheets_RowNum"] = record.source_row_num
        if record.running_balance:
            base_field_map["Running_Balance"] = record.running_balance

        for field_name, field_value in base_field_map.items():
            if field_name in id_set:
                fields[field_name] = field_value

        for extra_key, extra_value in record.extras.items():
            if extra_key in id_set:
                fields[extra_key] = extra_value
            elif extra_key in label_to_id:
                fields[label_to_id[extra_key]] = extra_value

        return fields
