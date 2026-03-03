from __future__ import annotations

from typing import Any

from gsheets_toolkit.utils import require_env

try:
    from google.oauth2 import service_account
    from googleapiclient.discovery import build
    from googleapiclient.errors import HttpError
except ModuleNotFoundError as exc:  # pragma: no cover - import guard
    raise RuntimeError(
        "Google Sheets dependencies are missing. Install google-api-python-client and google-auth."
    ) from exc


SHEETS_SCOPE = "https://www.googleapis.com/auth/spreadsheets"


class SheetsClient:
    def __init__(self, spreadsheet_id: str) -> None:
        if not spreadsheet_id.strip():
            raise RuntimeError("spreadsheet_id is required")
        self.spreadsheet_id = spreadsheet_id.strip()
        credentials_path = require_env("GOOGLE_APPLICATION_CREDENTIALS")
        credentials = service_account.Credentials.from_service_account_file(
            credentials_path,
            scopes=[SHEETS_SCOPE],
        )
        self._service = build("sheets", "v4", credentials=credentials, cache_discovery=False)

    def get_values(self, a1_range: str) -> list[list[str]]:
        try:
            response = (
                self._service.spreadsheets()
                .values()
                .get(spreadsheetId=self.spreadsheet_id, range=a1_range)
                .execute()
            )
            values = response.get("values", [])
            return values if isinstance(values, list) else []
        except HttpError as exc:
            raise RuntimeError(f"Failed to get values for range {a1_range}: {exc}") from exc

    def batch_update_values(self, updates: list[dict]) -> None:
        if not updates:
            return
        body = {
            "valueInputOption": "RAW",
            "data": updates,
        }
        try:
            (
                self._service.spreadsheets()
                .values()
                .batchUpdate(spreadsheetId=self.spreadsheet_id, body=body)
                .execute()
            )
        except HttpError as exc:
            raise RuntimeError(f"Failed to batch update values: {exc}") from exc

    def clear_values(self, a1_range: str) -> None:
        try:
            (
                self._service.spreadsheets()
                .values()
                .clear(spreadsheetId=self.spreadsheet_id, range=a1_range, body={})
                .execute()
            )
        except HttpError as exc:
            raise RuntimeError(f"Failed to clear values for range {a1_range}: {exc}") from exc

    def batch_update_requests(self, requests: list[dict]) -> None:
        if not requests:
            return
        body = {"requests": requests}
        try:
            (
                self._service.spreadsheets()
                .batchUpdate(spreadsheetId=self.spreadsheet_id, body=body)
                .execute()
            )
        except HttpError as exc:
            raise RuntimeError(f"Failed to apply spreadsheet requests: {exc}") from exc

    def get_spreadsheet(self) -> dict[str, Any]:
        try:
            return (
                self._service.spreadsheets()
                .get(
                    spreadsheetId=self.spreadsheet_id,
                    fields="sheets(properties(sheetId,title,gridProperties))",
                )
                .execute()
            )
        except HttpError as exc:
            raise RuntimeError(f"Failed to fetch spreadsheet metadata: {exc}") from exc

