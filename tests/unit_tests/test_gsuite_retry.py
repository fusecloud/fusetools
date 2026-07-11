"""Tests for Google Sheets retry and failure handling."""

from __future__ import annotations

import sys
import types
from typing import Any

import pandas as pd
import pytest

from fusetools import gsuite_tools
from fusetools.gsuite_tools import GSheets


class FakeRequest:
    def __init__(self, responses: list[Any]) -> None:
        self.responses = responses
        self.calls = 0

    def execute(self) -> Any:
        self.calls += 1
        response = self.responses.pop(0)
        if isinstance(response, Exception):
            raise response
        return response


def test_execute_google_request_retries_then_returns(monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]) -> None:
    request = FakeRequest([RuntimeError("token eyJabc.def.ghi"), {"ok": True}])
    sleeps: list[int] = []
    monkeypatch.setattr(gsuite_tools.time, "sleep", lambda seconds: sleeps.append(seconds))

    result = GSheets._execute_google_request(
        request=request,
        action_name="append Sheet1!A1",
        req_limit=2,
        sleep_seconds=3,
    )

    assert result == {"ok": True}
    assert request.calls == 2
    assert sleeps == [3]
    output = capsys.readouterr().out
    assert "Google Sheets append Sheet1!A1 failed attempt 1/2" in output
    assert "<redacted-jwt>" in output
    assert "eyJabc.def.ghi" not in output


def test_execute_google_request_raises_after_retries(monkeypatch: pytest.MonkeyPatch) -> None:
    exc = RuntimeError("still failing")
    request = FakeRequest([exc, exc])
    monkeypatch.setattr(gsuite_tools.time, "sleep", lambda seconds: None)

    with pytest.raises(RuntimeError, match="still failing"):
        GSheets._execute_google_request(
            request=request,
            action_name="get metadata",
            req_limit=2,
            sleep_seconds=0,
        )

    assert request.calls == 2


def test_update_google_sheet_df_uses_hardened_append(monkeypatch: pytest.MonkeyPatch) -> None:
    fake_service = FakeSheetsService({"updatedRows": 2})
    discovery = types.ModuleType("apiclient.discovery")
    discovery.build = lambda *_args, **_kwargs: fake_service  # type: ignore[attr-defined]
    apiclient = types.ModuleType("apiclient")
    apiclient.discovery = discovery  # type: ignore[attr-defined]
    monkeypatch.setitem(sys.modules, "apiclient", apiclient)
    monkeypatch.setitem(sys.modules, "apiclient.discovery", discovery)

    df = pd.DataFrame({"ticker": ["SPY"], "status": ["ok"]})
    result = GSheets.update_google_sheet_df(
        spreadsheet_id="spreadsheet-id",
        df=df,
        data_range="success_history!A2",
        credentials=object(),
        header=True,
    )

    assert result == {"updatedRows": 2}
    assert fake_service.append_kwargs == {
        "spreadsheetId": "spreadsheet-id",
        "range": "success_history!A2",
        "valueInputOption": "RAW",
        "insertDataOption": "INSERT_ROWS",
        "body": {"values": [["ticker", "status"], ["SPY", "ok"]]},
    }


class FakeSheetsService:
    def __init__(self, response: dict[str, Any]) -> None:
        self.request = FakeRequest([response])
        self.append_kwargs: dict[str, Any] | None = None

    def spreadsheets(self) -> "FakeSheetsService":
        return self

    def values(self) -> "FakeSheetsService":
        return self

    def append(self, **kwargs: Any) -> FakeRequest:
        self.append_kwargs = kwargs
        return self.request
