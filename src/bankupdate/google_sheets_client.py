from __future__ import annotations

from typing import Any

import gspread
from google.oauth2.service_account import Credentials

from .config import AppConfig


class GoogleSheetsClient:
    def __init__(self, config: AppConfig):
        scope = [
            "https://spreadsheets.google.com/feeds",
            "https://www.googleapis.com/auth/drive",
        ]
        credentials = Credentials.from_service_account_file(
            str(config.gsheet_credentials_path),
            scopes=scope,
        )
        self.client = gspread.authorize(credentials)
        self.spreadsheet = self.client.open_by_key(config.gsheet_id)
        self.worksheet = self.spreadsheet.worksheet(config.worksheet_name)

    def get_all_rows(self) -> list[dict[str, Any]]:
        all_values = self.worksheet.get_all_values()
        if not all_values:
            return []

        headers = all_values[0]
        rows: list[dict[str, Any]] = []
        for row_num, values in enumerate(all_values[1:], start=2):
            if not any(str(value).strip() for value in values):
                continue
            record: dict[str, Any] = {"Row_Num": row_num}
            for index, header in enumerate(headers):
                record[header] = values[index].strip() if index < len(values) and values[index] else ""
            rows.append(record)
        return rows

    def fetch_rows_from(self, start_row: int) -> list[dict[str, Any]]:
        return [row for row in self.get_all_rows() if int(row["Row_Num"]) >= start_row]

    def get_row_by_number(self, row_num: int) -> dict[str, Any] | None:
        for row in self.get_all_rows():
            if int(row["Row_Num"]) == row_num:
                return row
        return None

    def healthcheck(self) -> dict[str, Any]:
        rows = self.worksheet.get_all_values()
        header_count = len(rows[0]) if rows else 0
        observed_rows = max(len(rows) - 1, 0)
        return {
            "spreadsheet_title": self.spreadsheet.title,
            "worksheet_title": self.worksheet.title,
            "header_count": header_count,
            "observed_data_rows": observed_rows,
        }
