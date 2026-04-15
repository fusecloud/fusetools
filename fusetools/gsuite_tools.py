"""
Google Suite Tools.

|pic1|
    .. |pic1| image:: ../images_source/gsuite_tools/gsuitelogo1.png
        :width: 45%
"""

from __future__ import annotations

import base64
import io
import mimetypes
import os
import pickle
import shutil
import time
from email import encoders
from email.mime.application import MIMEApplication
from email.mime.audio import MIMEAudio
from email.mime.base import MIMEBase
from email.mime.image import MIMEImage
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from os.path import splitext
from typing import Any, Dict, List, Optional, Tuple

# https://developers.google.com/gmail/api/auth/scopes
SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive.file",
    "https://www.googleapis.com/auth/drive",
    "https://www.googleapis.com/auth/gmail.send",
    "https://www.googleapis.com/auth/gmail.readonly",
    "https://www.googleapis.com/auth/gmail.modify",
    "https://mail.google.com/",
    # 'https://www.googleapis.com/auth/documents.readonly'
]


# MARK: - GSheets
class GSheets:
    """
    Functions for interacting with Google Sheets.

    .. image:: ../images_source/gsuite_tools/googlesheets1.png

    """

    # MARK: - create_service_serv_acct
    @classmethod
    def create_service_serv_acct(
        cls, member_acct_email: str, token_path: Optional[str] = None, scopes: Optional[List[str]] = None
    ) -> Tuple[Any, Any]:
        """
        Creates a GSheets authenticated credentials object.

        :param member_acct_email: GSuite service acct email address.
        :param token_path: Path to GSuite authentication token.
        :param scopes: Optional list of OAuth scopes. Defaults to module-level SCOPES.
        :return: Return GSheets authenticated credentials object.
        """
        import os

        from apiclient.discovery import build
        from oauth2client.service_account import ServiceAccountCredentials

        token_path = token_path or os.environ.get("GSUITE_TOKEN_PATH")
        _scopes = scopes or ([s.strip() for s in os.environ["GSUITE_SCOPES"].split(",")] if os.environ.get("GSUITE_SCOPES") else SCOPES)
        credentials = ServiceAccountCredentials.from_json_keyfile_name(filename=token_path, scopes=_scopes)

        credentials = credentials.create_delegated(member_acct_email)

        service = build("sheets", "v4", credentials=credentials)
        return service, credentials

    # MARK: - create_service_serv_acct_dict
    @classmethod
    def create_service_serv_acct_dict(cls, member_acct_email: str, dict_creds: Dict[str, Any], scopes: Optional[List[str]] = None) -> Tuple[Any, Any]:
        """
        Creates a GDrive authenticated credentials object.

        :param member_acct_email: GDrive service acct email address.
        :param token_path: Path to GDrive authentication token.
        :param scopes: Optional list of OAuth scopes. Defaults to module-level SCOPES.
        :return: Return GDrive authenticated credentials object.
        """
        import os

        from apiclient.discovery import build
        from oauth2client.service_account import ServiceAccountCredentials

        _scopes = scopes or ([s.strip() for s in os.environ["GSUITE_SCOPES"].split(",")] if os.environ.get("GSUITE_SCOPES") else SCOPES)
        credentials = ServiceAccountCredentials.from_json_keyfile_dict(keyfile_dict=dict_creds, scopes=_scopes)

        credentials = credentials.create_delegated(member_acct_email)

        service = build("sheets", "v4", credentials=credentials)
        return service, credentials

    # MARK: - make_google_sheet
    @classmethod
    def make_google_sheet(cls, ss_name: str, credentials: Any, req_limit: int = 1) -> Optional[str]:
        """
        Creates a Google Sheet in one's GSuite account.

        :param ss_name: Name of the Google Sheet to be created.
        :param credentials: GSuite credentials object.
        :return: Id of newly created Google Sheet.
        """
        from apiclient import discovery

        SHEETS = discovery.build("sheets", "v4", credentials=credentials)
        data = {"properties": {"title": ss_name}}

        req_count = 0
        while req_count < req_limit:
            try:
                sheet = SHEETS.spreadsheets().create(body=data).execute()
                sheet_id = sheet.get("spreadsheetId")
                print(f"Created wb: {ss_name}")
                return sheet_id
            except:
                print("Exception...sleeping")
                time.sleep(3)
                req_count += 1

    # MARK: - add_google_sheet_tab
    @classmethod
    def add_google_sheet_tab(cls, spreadsheet_id: str, tab_name: str, credentials: Any, req_limit: int = 1) -> Optional[Any]:
        """
        Adds a tab to a Google Sheet.

        :param spreadsheet_id: Id of Google Sheet to add tab to.
        :param tab_name: Name of the tab to be added.
        :param credentials: GSuite credentials object.
        :return: Result object for API call.
        """
        from apiclient.discovery import build

        data = {"requests": [{"addSheet": {"properties": {"title": tab_name}}}]}
        service = build("sheets", "v4", credentials=credentials)

        req_count = 0
        while req_count < req_limit:
            try:
                res = (
                    service.spreadsheets()
                    .batchUpdate(
                        spreadsheetId=spreadsheet_id,
                        body=data,
                    )
                    .execute()
                )
                print(f"Added sheet: {tab_name}")
                return res

            except:
                print("Exception...sleeping")
                time.sleep(3)
                req_count += 1

    # MARK: - get_google_sheet
    @classmethod
    def get_google_sheet(
        cls,
        spreadsheet_id: str,
        range_name: str,
        credentials: Any,
        tab_name: Any = False,
        req_limit: int = 1,
    ) -> Any:
        """
        Gets data from a Google Sheet.

        :param spreadsheet_id: Id of Google Sheet to retrieve.
        :param range_name: Row/Column of Sheet range to retrieve (ex: A1:A99).
        :param tab_name: Name of tab to pull data from (optional).
        :param credentials: GSuite credentials object.
        :return: Pandas DataFrame of retrieved data.
        """
        import pandas as pd
        from apiclient.discovery import build

        service = build("sheets", "v4", credentials=credentials)

        req_count = 0
        while req_count < req_limit:
            try:
                # Call the Sheets API
                sheet = service.spreadsheets()
                if tab_name:
                    gsheet = (
                        sheet.values()
                        .get(
                            spreadsheetId=spreadsheet_id,
                            range=tab_name + "!" + range_name,
                        )
                        .execute()
                    )

                else:
                    gsheet = sheet.values().get(spreadsheetId=spreadsheet_id, range=range_name).execute()
                break
            except:
                print("Exception...sleeping")
                time.sleep(3)
                req_count += 1

        header = gsheet.get("values")[0]
        values = gsheet.get("values")[1:]

        if len(header) > len(values[0]):
            values_new = []
            for idx, v in enumerate(values):
                values_new.append(v + ["" for x in header[len(v) :]])
        else:
            values_new = values

        df = pd.DataFrame(values_new)
        df = df[df.columns[: len(header)]]
        df.columns = header

        return df

    # MARK: - bulk_add_google_sheet_comment
    @classmethod
    def bulk_add_google_sheet_comment(
        cls,
        spreadsheet_id: str,
        request_list: List[Any],
        credentials: Any,
        req_limit: int = 1,
    ) -> Optional[Any]:
        """

        :param spreadsheet_id:
        :param tab_id:
        :param credentials:
        :return:
        """
        from apiclient.discovery import build

        service = build("sheets", "v4", credentials=credentials)
        data = {"requests": request_list}

        req_count = 0
        while req_count < req_limit:
            try:
                res = (
                    service.spreadsheets()
                    .batchUpdate(
                        spreadsheetId=spreadsheet_id,
                        body=data,
                    )
                    .execute()
                )
                return res
            except:
                print("Exception....sleeping")
                time.sleep(3)
                req_count += 1

    # MARK: - add_google_sheet_comment
    @classmethod
    def add_google_sheet_comment(
        cls,
        spreadsheet_id: str,
        tab_id: int,
        note_contents: str,
        start_row_idx: int,
        end_row_idx: int,
        start_col_idx: int,
        end_col_idx: int,
        credentials: Any,
        req_limit: int = 1,
    ) -> Optional[Any]:
        """

        :param spreadsheet_id:
        :param tab_id:
        :param credentials:
        :return:
        """
        from apiclient.discovery import build

        service = build("sheets", "v4", credentials=credentials)
        data = {
            "requests": [
                {
                    "updateCells": {
                        "range": {
                            "sheetId": tab_id,
                            "startRowIndex": start_row_idx,
                            "endRowIndex": end_row_idx,
                            "startColumnIndex": start_col_idx,
                            "endColumnIndex": end_col_idx,
                        },
                        "rows": [{"values": [{"note": note_contents}]}],
                        "fields": "note",
                    }
                }
            ]
        }
        req_count = 0
        while req_count < req_limit:
            try:
                res = (
                    service.spreadsheets()
                    .batchUpdate(
                        spreadsheetId=spreadsheet_id,
                        body=data,
                    )
                    .execute()
                )
                return res
            except:
                print("Exception....sleeping")
                time.sleep(3)
                req_count += 1

    # MARK: - freeze_rows_cols
    @classmethod
    def freeze_rows_cols(
        cls,
        spreadsheet_id: str,
        tab_id: int,
        freeze_idx: int,
        credentials: Any,
        rows: bool = True,
        req_limit: int = 1,
    ) -> Optional[Any]:
        """
        Freezes the rows of a given Google Sheet.

        :param rows:
        :param freeze_idx:
        :param spreadsheet_id: Id of Google Sheet to retrieve.
        :param tab_id: Id of tab to modify.
        :param freeze_row: Spreadsheet row to freeze.
        :param credentials: GSuite credentials object.
        :return: Result object for API call.
        """
        from apiclient.discovery import build

        service = build("sheets", "v4", credentials=credentials)

        data = {
            "requests": [
                {
                    "updateSheetProperties": {
                        "properties": {
                            "sheetId": tab_id,
                            "gridProperties": {"frozenRowCount" if rows else "frozenColumnCount": freeze_idx},
                        },
                        "fields": "gridProperties.frozenRowCount" if rows else "gridProperties.frozenColumnCount",
                    }
                }
            ]
        }

        req_count = 0
        while req_count < req_limit:
            try:
                res = (
                    service.spreadsheets()
                    .batchUpdate(
                        spreadsheetId=spreadsheet_id,
                        body=data,
                    )
                    .execute()
                )
                return res
            except:
                print("Exception....sleeping")
                time.sleep(3)
                req_count += 1

    # MARK: - group_sheet_cols_rows
    @classmethod
    def group_sheet_cols_rows(
        cls,
        spreadsheet_id: str,
        tab_id: int,
        start_idx: int,
        end_idx: int,
        credentials: Any,
        rows_columns: str = "ROWS",
        req_limit: int = 1,
    ) -> Optional[Any]:
        """

        :param spreadsheet_id:
        :param tab_id:
        :param start_idx:
        :param end_idx:
        :param credentials:
        :param rows_columns:
        :return:
        """
        from apiclient.discovery import build

        service = build("sheets", "v4", credentials=credentials)
        data = {
            "requests": [
                {
                    "addDimensionGroup": {
                        "range": {
                            "dimension": rows_columns,
                            "sheetId": tab_id,
                            "startIndex": start_idx,
                            "endIndex": end_idx,
                        }
                    }
                }
            ]
        }

        req_count = 0
        while req_count < req_limit:
            try:
                res = (
                    service.spreadsheets()
                    .batchUpdate(
                        spreadsheetId=spreadsheet_id,
                        body=data,
                    )
                    .execute()
                )
                return res
            except:
                print("Exception....sleeping")
                time.sleep(3)
                req_count += 1

    # MARK: - update_cell_background_color
    @classmethod
    def update_cell_background_color(
        cls,
        spreadsheet_id: str,
        sheet_id: int,
        row_idx_start: int,
        row_idx_end: int,
        col_idx_start: int,
        credentials: Any,
        color_dict: Dict[str, Any],
        cell_or_row: str = "CELL",
        col_idx_end: Any = False,
        req_limit: int = 1,
    ) -> Optional[Any]:
        """

        :param spreadsheet_id:
        :param sheet_id:
        :param row_idx_start:
        :param row_idx_end:
        :param col_idx_start:
        :param col_idx_end:
        :param credentials:
        :param dimension:
        :return:
        """
        from apiclient.discovery import build

        service = build("sheets", "v4", credentials=credentials)

        if cell_or_row == "CELL":
            data = {
                "requests": [
                    {
                        "updateCells": {
                            "range": {
                                "sheetId": sheet_id,
                                "startRowIndex": row_idx_start,
                                "endRowIndex": row_idx_end,
                                "startColumnIndex": col_idx_start,
                                "endColumnIndex": col_idx_end,
                            },
                            "rows": [{"values": [{"userEnteredFormat": {"backgroundColor": color_dict}}]}],
                            "fields": "userEnteredFormat.backgroundColor",
                        }
                    }
                ]
            }

        else:
            data = {
                "requests": [
                    {
                        "repeatCell": {
                            "range": {
                                "sheetId": sheet_id,
                                "startRowIndex": row_idx_start,
                                "endRowIndex": row_idx_end,
                                "startColumnIndex": col_idx_start,
                            },
                            "cell": {"userEnteredFormat": {"backgroundColor": color_dict}},
                            "fields": "userEnteredFormat.backgroundColor",
                        }
                    }
                ]
            }

        req_count = 0
        while req_count < req_limit:
            try:
                res = (service.spreadsheets().batchUpdate(spreadsheetId=spreadsheet_id, body=data)).execute()
                return res
            except:
                print("Exception....sleeping")
                time.sleep(3)
                req_count += 1

    # MARK: - bulk_update_cell_background_color
    @classmethod
    def bulk_update_cell_background_color(
        cls,
        spreadsheet_id: str,
        credentials: Any,
        request_list: List[Any],
        req_limit: int = 1,
    ) -> Optional[Any]:
        """

        :param spreadsheet_id:
        :param sheet_id:
        :param row_idx_start:
        :param row_idx_end:
        :param col_idx_start:
        :param col_idx_end:
        :param credentials:
        :param dimension:
        :return:
        """
        from apiclient.discovery import build

        service = build("sheets", "v4", credentials=credentials)

        data = {"requests": request_list}

        req_count = 0
        while req_count < req_limit:
            try:
                res = (service.spreadsheets().batchUpdate(spreadsheetId=spreadsheet_id, body=data)).execute()
                return res
            except:
                print("Exception....sleeping")
                time.sleep(3)
                req_count += 1

    # MARK: - drop_duplicates
    @classmethod
    def drop_duplicates(
        cls,
        spreadsheet_id: str,
        sheet_id: int,
        credentials: Any,
        dup_idx_start: int,
        dup_idx_end: int,
        row_idx_start: int,
        row_idx_end: int,
        col_idx_start: int,
        col_idx_end: int,
        rows_columns: str = "COLUMNS",
        req_limit: int = 1,
    ) -> Optional[Any]:
        """

        :param spreadsheet_id:
        :param sheet_id:
        :param credentials:
        :param dup_idx_start:
        :param dup_idx_end:
        :param row_idx_start:
        :param row_idx_end:
        :param col_idx_start:
        :param col_idx_end:
        :param rows_columns:
        :return:
        """
        from apiclient.discovery import build

        service = build("sheets", "v4", credentials=credentials)

        data = {
            "requests": {
                "deleteDuplicates": {
                    # Removes rows within this range that contain values in the specified columns that are duplicates of values in any previous row. Rows with identical values but different letter cases, formatting, or formulas are considered to be duplicates. This request also removes duplicate rows hidden from view (for example, due to a filter). When removing duplicates, the first instance of each duplicate row scanning from the top downwards is kept in the resulting range. Content outside of the specified range isn't removed, and rows considered duplicates do not have to be adjacent to each other in the range. # Removes rows containing duplicate values in specified columns of a cell range.
                    "comparisonColumns": [
                        # The columns in the range to analyze for duplicate values. If no columns are selected then all columns are analyzed for duplicates.
                        {
                            # A range along a single dimension on a sheet. All indexes are zero-based. Indexes are half open: the start index is inclusive and the end index is exclusive. Missing indexes indicate the range is unbounded on that side.
                            "dimension": rows_columns,  # The dimension of the span.
                            "endIndex": dup_idx_end,  # The end (exclusive) of the span, or not set if unbounded.
                            "sheetId": sheet_id,  # The sheet this span is on.
                            "startIndex": dup_idx_start,  # The start (inclusive) of the span, or not set if unbounded.
                        },
                    ],
                    "range": {
                        # A range on a sheet. All indexes are zero-based. Indexes are half open, i.e. the start index is inclusive and the end index is exclusive -- [start_index, end_index). Missing indexes indicate the range is unbounded on that side. For example, if `"Sheet1"` is sheet ID 0, then: `Sheet1!A1:A1 == sheet_id: 0, start_row_index: 0, end_row_index: 1, start_column_index: 0, end_column_index: 1` `Sheet1!A3:B4 == sheet_id: 0, start_row_index: 2, end_row_index: 4, start_column_index: 0, end_column_index: 2` `Sheet1!A:B == sheet_id: 0, start_column_index: 0, end_column_index: 2` `Sheet1!A5:B == sheet_id: 0, start_row_index: 4, start_column_index: 0, end_column_index: 2` `Sheet1 == sheet_id:0` The start index must always be less than or equal to the end index. If the start index equals the end index, then the range is empty. Empty ranges are typically not meaningful and are usually rendered in the UI as `#REF!`. # The range to remove duplicates rows from.
                        "endColumnIndex": col_idx_end,
                        # The end column (exclusive) of the range, or not set if unbounded.
                        "endRowIndex": row_idx_end,  # The end row (exclusive) of the range, or not set if unbounded.
                        "sheetId": sheet_id,  # The sheet this range is on.
                        "startColumnIndex": col_idx_start,
                        # The start column (inclusive) of the range, or not set if unbounded.
                        "startRowIndex": row_idx_start,
                        # The start row (inclusive) of the range, or not set if unbounded.
                    },
                }
            }
        }

        req_count = 0
        while req_count < req_limit:
            try:
                res = (service.spreadsheets().batchUpdate(spreadsheetId=spreadsheet_id, body=data)).execute()
                return res
            except:
                print("Exception....sleeping")
                time.sleep(3)
                req_count += 1

    # MARK: - update_google_sheet_val
    @classmethod
    def update_google_sheet_val(
        cls,
        spreadsheet_id: str,
        tab_id: int,
        val: str,
        row: int,
        col: int,
        credentials: Any,
        req_limit: int = 1,
    ) -> Optional[Any]:
        """
        Updates a google spreadsheet cell with a value.

        :param spreadsheet_id: Id of spreadsheet to update.
        :param tab_id: Id of spreadsheet tab to update.
        :param val: Value to update spreadsheet cell with.
        :param row: Row of cell to update.
        :param col: Column of cell to update.
        :param credentials: GSuite credentials object.
        :return: API response.
        """
        from apiclient.discovery import build

        service = build("sheets", "v4", credentials=credentials)

        data = {
            "requests": {
                "updateCells": {
                    "rows": [{"values": [{"userEnteredValue": {"formulaValue": val}}]}],
                    "fields": "userEnteredValue",
                    "start": {"sheetId": tab_id, "rowIndex": row, "columnIndex": col},
                }
            }
        }

        req_count = 0
        while req_count < req_limit:
            try:
                res = (service.spreadsheets().batchUpdate(spreadsheetId=spreadsheet_id, body=data)).execute()
                return res
            except:
                print("Exception....sleeping")
                time.sleep(3)
                req_count += 1

    # MARK: - update_google_sheet_df
    @classmethod
    def update_google_sheet_df(
        cls,
        spreadsheet_id: str,
        df: Any,
        data_range: str,
        credentials: Any,
        header: bool = False,
        req_limit: int = 1,
    ) -> Optional[Any]:
        """
        Uploads a Pandas DataFrame to a Google Sheet.

        :param spreadsheet_id: ID of spreadsheet to update.
        :param df: Pandas DataFrame to update spreadsheet with.
        :param data_range: Spreadsheet range to insert DataFrame into.
        :param credentials: GSuite credentials object.
        :return: API response.
        """
        from apiclient.discovery import build

        service = build("sheets", "v4", credentials=credentials)
        value_input_option = "USER_ENTERED"

        write_df = []

        for idx, row in df.iterrows():
            write_df.append(df.iloc[idx].tolist())

        if header:
            write_df = [df.columns.tolist()] + write_df

        body = dict({"values": write_df})

        req_count = 0
        while req_count < req_limit:
            try:
                res = (
                    service.spreadsheets()
                    .values()
                    .append(
                        spreadsheetId=spreadsheet_id,
                        range=data_range,
                        valueInputOption=value_input_option,
                        # insertDataOption=insert_data_option,
                        body=body,
                    )
                    .execute()
                )
                return res
            except:
                print("Exception...sleeping")
                time.sleep(3)
                req_count += 1

    # MARK: - clear_google_sheet_data
    @classmethod
    def clear_google_sheet_data(cls, spreadsheet_id: str, range: str, credentials: Any, req_limit: int = 1) -> Optional[Any]:
        from apiclient.discovery import build

        service = build("sheets", "v4", credentials=credentials)

        body = {}
        req_count = 0
        while req_count < req_limit:
            try:
                res = service.spreadsheets().values().clear(spreadsheetId=spreadsheet_id, range=range, body=body).execute()
                return res
            except:
                print("Exception...sleeping")
                time.sleep(3)
                req_count += 1

    # MARK: - delete_google_sheet_data
    @classmethod
    def delete_google_sheet_data(
        cls,
        spreadsheet_id: str,
        sheet_id: int,
        idx_start: int,
        idx_end: int,
        credentials: Any,
        dimension: str = "ROWS",
        req_limit: int = 1,
    ) -> Optional[Any]:
        """
        Deletes data from a Google Sheet.

        :param spreadsheet_id: ID of spreadsheet to update.
        :param idx_start: Starting index of row/column for range to delete.
        :param idx_end: Ending index of row/column for range to delete.
        :param credentials:
        :param dimension:
        :return:
        """
        from apiclient.discovery import build

        service = build("sheets", "v4", credentials=credentials)
        spreadsheet_data = [
            {
                "deleteDimension": {
                    "range": {
                        "sheetId": sheet_id,
                        "dimension": dimension,
                        "startIndex": idx_start,
                        "endIndex": idx_end,
                    }
                }
            }
        ]

        request = {"requests": spreadsheet_data}
        req_count = 0
        while req_count < req_limit:
            try:
                res = service.spreadsheets().batchUpdate(spreadsheetId=spreadsheet_id, body=request).execute()
                return res
            except:
                print("Exception...sleeping")
                time.sleep(3)
                req_count += 1

    # MARK: - get_google_sheet_metadata
    @classmethod
    def get_google_sheet_metadata(
        cls,
        spreadsheet_id: str,
        credentials: Any,
        include_grid_data: bool = False,
        ranges: Any = False,
        req_limit: int = 1,
    ) -> Optional[Any]:
        """

        :param include_grid_data:
        :param spreadsheet_id:
        :param credentials:
        :param ranges:
        :return:
        """
        from apiclient.discovery import build

        service = build("sheets", "v4", credentials=credentials)
        req_count = 0
        while req_count < req_limit:
            try:
                request = service.spreadsheets().get(
                    spreadsheetId=spreadsheet_id,
                    includeGridData=include_grid_data,
                    ranges=ranges,
                )

                response = request.execute()
                return response
            except:
                print("Exception...sleeping")
                time.sleep(3)
                req_count += 1

    # MARK: - get_google_sheet_tabs
    @classmethod
    def get_google_sheet_tabs(cls, spreadsheet_id: str, credentials: Any, req_limit: int = 1) -> Tuple[List[str], List[int]]:
        """
        Get the names and IDs of tabs for a given Google Sheet.

        :param spreadsheet_id: ID of spreadsheet to update.
        :param credentials: GSuite credentials object.
        :return: Names and IDs of tabs for a given Google Sheet.
        """
        from apiclient.discovery import build

        service = build("sheets", "v4", credentials=credentials)
        req_count = 0
        while req_count < req_limit:
            try:
                sheet_metadata = service.spreadsheets().get(spreadsheetId=spreadsheet_id).execute()
                break
            except:
                print("Exception...sleeping")
                time.sleep(3)
                req_count += 1

        tab_names = []
        tab_ids = []
        for idx, e in enumerate(sheet_metadata.get("sheets")):
            tab_names.append(e.get("properties").get("title"))
            tab_ids.append(e.get("properties").get("sheetId"))

        return tab_names, tab_ids

    # MARK: - sort_google_sheet
    @classmethod
    def sort_google_sheet(
        cls,
        spreadsheet_id: str,
        credentials: Any,
        tab_id: int,
        start_row_idx: int,
        end_row_idx: int,
        start_col_idx: int,
        end_col_idx: int,
        sort_idx_list: List[int],
        sort_order_list: List[str],
        req_limit: int = 1,
    ) -> Optional[Any]:
        from apiclient.discovery import build

        service = build("sheets", "v4", credentials=credentials)

        sort_spec_list = [{"dimensionIndex": x, "sortOrder": y} for x, y in zip(sort_idx_list, sort_order_list)]

        data = {
            "requests": [
                {
                    "sortRange": {
                        "range": {
                            "sheetId": tab_id,
                            "startRowIndex": start_row_idx,
                            "endRowIndex": end_row_idx,
                            "startColumnIndex": start_col_idx,
                            "endColumnIndex": end_col_idx,
                        },
                        "sortSpecs": sort_spec_list,
                    }
                }
            ]
        }

        req_count = 0
        while req_count < req_limit:
            try:
                res = (service.spreadsheets().batchUpdate(spreadsheetId=spreadsheet_id, body=data)).execute()
                return res
            except:
                print("Exception....sleeping")
                time.sleep(3)
                req_count += 1

    # MARK: - delete_google_sheet_tab
    #     todo: delete google sheet
    @classmethod
    def delete_google_sheet_tab(cls, spreadsheet_id: str, tab_id: int, credentials: Any, req_limit: int = 1) -> Optional[Any]:
        """
        Adds a tab to a Google Sheet.

        :param spreadsheet_id: Id of Google Sheet to add tab to.
        :param tab_name: Name of the tab to be added.
        :param credentials: GSuite credentials object.
        :return: Result object for API call.
        """
        from apiclient.discovery import build

        data = {"requests": {"deleteSheet": {"sheetId": tab_id}}}
        service = build("sheets", "v4", credentials=credentials)

        req_count = 0
        while req_count < req_limit:
            try:
                res = (
                    service.spreadsheets()
                    .batchUpdate(
                        spreadsheetId=spreadsheet_id,
                        body=data,
                    )
                    .execute()
                )
                return res
            except:
                print("Exception...sleeping")
                time.sleep(3)
                req_count += 1

            print(f"Deleted sheet: {tab_id}")


# MARK: - GDrive
class GDrive:
    """
    Functions for interacting with Google Drive.

    .. image:: ../images_source/gsuite_tools/googledrive1.jpeg

    """

    # MARK: - create_service_serv_acct
    @classmethod
    def create_service_serv_acct(
        cls, member_acct_email: str, token_path: Optional[str] = None, scopes: Optional[List[str]] = None
    ) -> Tuple[Any, Any]:
        """
        Creates a GDrive authenticated credentials object.

        :param member_acct_email: GDrive service acct email address.
        :param token_path: Path to GDrive authentication token.
        :param scopes: Optional list of OAuth scopes. Defaults to module-level SCOPES.
        :return: Return GDrive authenticated credentials object.
        """
        import os

        from apiclient.discovery import build
        from oauth2client.service_account import ServiceAccountCredentials

        token_path = token_path or os.environ.get("GSUITE_TOKEN_PATH")
        _scopes = scopes or ([s.strip() for s in os.environ["GSUITE_SCOPES"].split(",")] if os.environ.get("GSUITE_SCOPES") else SCOPES)
        credentials = ServiceAccountCredentials.from_json_keyfile_name(filename=token_path, scopes=_scopes)

        credentials = credentials.create_delegated(member_acct_email)

        service = build("drive", "v3", credentials=credentials)
        return service, credentials

    # MARK: - create_service_serv_acct_dict
    @classmethod
    def create_service_serv_acct_dict(cls, member_acct_email: str, dict_creds: Dict[str, Any], scopes: Optional[List[str]] = None) -> Tuple[Any, Any]:
        """
        Creates a GDrive authenticated credentials object.

        :param member_acct_email: GDrive service acct email address.
        :param token_path: Path to GDrive authentication token.
        :param scopes: Optional list of OAuth scopes. Defaults to module-level SCOPES.
        :return: Return GDrive authenticated credentials object.
        """
        import os

        from apiclient.discovery import build
        from oauth2client.service_account import ServiceAccountCredentials

        _scopes = scopes or ([s.strip() for s in os.environ["GSUITE_SCOPES"].split(",")] if os.environ.get("GSUITE_SCOPES") else SCOPES)
        credentials = ServiceAccountCredentials.from_json_keyfile_dict(keyfile_dict=dict_creds, scopes=_scopes)

        credentials = credentials.create_delegated(member_acct_email)

        service = build("drive", "v3", credentials=credentials)
        return service, credentials

    # MARK: - authorize_credentials
    @classmethod
    def authorize_credentials(cls, cred_path: Optional[str] = None, token_path: Optional[str] = None, scopes: Optional[List[str]] = None) -> Any:
        """
        Creates an authorized credentials object for Google Drive.

        :param cred_path: Local path to GSuite credentials object.
        :param token_path: Local path to GSuite authorization token.
        :param scopes: Optional list of OAuth scopes. Defaults to module-level SCOPES.
        :return: Authorized credentials object for GSuite.
        """
        import os

        from google.auth.transport.requests import Request
        from google_auth_oauthlib.flow import InstalledAppFlow

        cred_path = cred_path or os.environ.get("GSUITE_TOKEN_PATH")

        cred_path = cred_path or os.environ.get("GSUITE_TOKEN_PATH")

        creds = None
        if os.path.exists(token_path):
            with open(token_path, "rb") as token:
                creds = pickle.load(token)
            # If there are no (valid) credentials available, let the user log in.
        if not creds or not creds.valid:
            if creds and creds.expired and creds.refresh_token:
                creds.refresh(Request())
            else:
                flow = InstalledAppFlow.from_client_secrets_file(
                    cred_path, scopes or ([s.strip() for s in os.environ["GSUITE_SCOPES"].split(",")] if os.environ.get("GSUITE_SCOPES") else SCOPES)
                )
                creds = flow.run_local_server(port=0)
                # Save the credentials for the next run
            with open(token_path, "wb") as token:
                pickle.dump(creds, token)
        return creds

    # MARK: - download_file
    @classmethod
    def download_file(cls, file_id: str, file_name: str, credentials: Any, save_local: bool = False) -> bytes:
        """
        Downloads a file from a Google Drive account.

        :param file_name: Name of file to download from Google Drive.
        :param save_local: whether to return file in bytes or save locally (default is bytes).
        :param file_id: ID for Google Drive file.
        :param credentials: GSuite credentials object.
        :return: Downloaded file from Google Drive.
        """
        from apiclient.discovery import build
        from googleapiclient.http import MediaIoBaseDownload

        drive_service = build("drive", "v3", credentials=credentials)
        request = drive_service.files().get_media(
            fileId=file_id,
            # mimeType=content_type
        )
        fh = io.BytesIO()
        downloader = MediaIoBaseDownload(fh, request)
        done = False
        while done is False:
            status, done = downloader.next_chunk()
            print("Download %d%%." % int(status.progress() * 100))

        # https://stackoverflow.com/questions/54137790/convert-from-io-bytesio-to-a-bytes-like-object-in-python3-6
        # write to file handler object
        fh.seek(0)
        file_bytes = fh.read()
        # The file has been downloaded into RAM, now save it in a file
        if save_local:
            fh.seek(0)
            with open(file_name, "wb") as f:
                shutil.copyfileobj(fh, f, length=131072)
        return file_bytes

    # MARK: - get_all_google_items
    @classmethod
    def get_all_google_items(
        cls,
        credentials: Any,
        page_size: Optional[int] = None,
        folder_id: Optional[str] = None,
    ) -> Any:
        """
        Get all files in a Google Drive (or folder if specified).

        :param page_size:
        :param limit:
        :param credentials: GSuite credentials object.
        :param folder_id: GDrive folder to search in (optional)
        :return: Pandas DataFrame of files in a Google Drive.
        """
        import pandas as pd
        from apiclient import discovery

        DRIVE = discovery.build("drive", "v3", credentials=credentials)

        res_all = []

        res = (
            DRIVE.files()
            .list(
                q="'" + folder_id + "' in parents" if folder_id else None,
                pageSize=page_size,
                fields="nextPageToken, files(id, name)",
            )
            .execute()
        )

        res_all += res.get("files")

        while res.get("nextPageToken"):
            res = (
                DRIVE.files()
                .list(
                    q="'" + folder_id + "' in parents" if folder_id else None,
                    pageSize=page_size,
                    fields="nextPageToken, files(id, name)",
                    pageToken=res["nextPageToken"],
                )
                .execute()
            )

            res_all += res.get("files")

        df = pd.DataFrame.from_dict(res_all)

        return df

    # MARK: - create_upload_folder
    @classmethod
    def create_upload_folder(
        cls,
        folder_name: str,
        credentials: Any,
        overwrite_folder: bool = False,
        parent_id: Optional[str] = None,
    ) -> Any:
        """
        Creates a folder in Google Drive.

        :param folder_name: Name of folder to create.
        :param credentials: GSuite credentials object.
        :param overwrite_folder: Whether or not to delete and re-create the folder.
        :param parent_id: Id of parent folder (optional).
        :return: Created folder information.
        """
        from apiclient.discovery import build

        # Create a folder on Drive, returns the newly created folders ID
        drive_service = build("drive", "v3", credentials=credentials)

        response = (
            drive_service.files()
            .list(
                q="mimeType = 'application/vnd.google-apps.folder'",
                fields="nextPageToken, files(id, name)",
            )
            .execute()
        )

        if overwrite_folder:
            for file in response.get("files", []):
                if folder_name == file.get("name"):
                    print("Overwriting file: %s (%s)" % (file.get("name"), file.get("id")))
                    drive_service.files().delete(fileId=file.get("id")).execute()
                    break

                page_token = response.get("nextPageToken", None)

                if page_token is None:
                    break

        body = {"name": folder_name, "mimeType": "application/vnd.google-apps.folder"}
        if parent_id:
            body["parents"] = [parent_id]
        folder = drive_service.files().create(body=body).execute()

        return folder

    # MARK: - upload_files
    @classmethod
    def upload_files(
        cls,
        folder_id: str,
        upload_filepaths: List[Any],
        credentials: Any,
        upload_filenames: Any = False,
        upload_from_memory: bool = False,
    ) -> List[Any]:
        """
        Uploads files to a folder on Google Drive.

        :param folder_id: ID of folder to upload files into.
        :param credentials: GSuite credentials object.
        :param upload_filepaths: Filepaths for files to upload.
        :param upload_filenames: Filenames for files, must match length of filepaths (optional). Otherwise uses filepath names.
        :return: Confirmation of files being uploaded.
        """
        from apiclient.discovery import build
        from googleapiclient.http import MediaFileUpload, MediaIoBaseUpload

        drive_service = build("drive", "v3", credentials=credentials)

        files = []
        for idx, file in enumerate(upload_filepaths):
            # name = os.fsdecode(file)
            # filename, file_extension = splitext(name, )
            # if upload_filenames:
            filename, file_extension = splitext(
                upload_filenames[idx],
            )

            # setting mimeType for each file
            # more types available here:
            # https://developer.mozilla.org/en-US/docs/Web/HTTP/Basics_of_HTTP/MIME_types/Common_types

            if file_extension == ".pdf":
                mimeType = "application/pdf"

            elif ".gz" in file_extension:
                mimeType = "application/gzip"

            elif ".zip" in file_extension:
                mimeType = "application/zip"

            elif file_extension == ".doc":
                mimeType = "application/msword"

            elif file_extension == ".docx":
                mimeType = "application/vnd.openxmlformats-officedocument.wordprocessingml.document"

            elif file_extension == ".ppt":
                mimeType = "application/vnd.ms-powerpoint"

            elif file_extension == ".pptx":
                mimeType = "application/vnd.openxmlformats-officedocument.presentationml.presentation"

            elif ".csv" in file_extension:
                mimeType = "text/csv"

            elif ".txt" in file_extension:
                mimeType = "text/plain"

            elif file_extension == ".xls":
                mimeType = "application/vnd.ms-excel"

            elif file_extension == ".xlsx":
                mimeType = "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"

            elif ".jpg" in file_extension or ".jpeg" in file_extension:
                mimeType = "image/jpeg"

            elif ".png" in file_extension:
                mimeType = "image/png"

            elif ".tif" in file_extension or ".tiff" in file_extension:
                mimeType = "image/tiff"

            elif ".gif" in file_extension:
                mimeType = "image/gif"

            elif ".json" in file_extension:
                mimeType = "application/json"

            elif ".mp3" in file_extension:
                mimeType = "audio/mpeg"

            elif ".mpeg" in file_extension:
                mimeType = "video/mpeg"

            elif ".wav" in file_extension:
                mimeType = "video/wav"

            else:
                print(f"File type for {file} not mapped...")
                continue

            file_metadata = {
                "name": filename + file_extension,
                "mimeType": mimeType,
                "parents": [folder_id],
            }

            if upload_from_memory:
                try:
                    fh = io.BytesIO(file)
                    media = MediaIoBaseUpload(fh, mimetype=mimeType, chunksize=1024 * 1024, resumable=True)

                    file_ = drive_service.files().create(body=file_metadata, media_body=media, fields="id").execute()

                    print(f"""file upload success for: {file}""")
                    files.append(file_)

                except Exception as e:
                    print(str(e))
                    print(f"""file upload failed for: {file}, method: memory""")
                    continue

            else:
                try:
                    media = MediaFileUpload(file, resumable=True)

                    file_ = drive_service.files().create(body=file_metadata, media_body=media, fields="id").execute()

                    print(f"""file upload success for: {file}""")
                    files.append(file_)
                except Exception as e:
                    print(str(e))
                    print(f"""file upload failed for: {file}, method: disk""")
                    continue

        return files

    # MARK: - get_file_revisions
    @classmethod
    def get_file_revisions(cls, file_id: str, credentials: Any) -> Any:
        """

        :param file_id:
        :param credentials:
        :return:
        """
        from apiclient.discovery import build

        # https://developers.google.com/drive/api/v3/reference/revisions/list
        drive_service = build("drive", "v3", credentials=credentials)

        r = drive_service.revisions().list(pageSize=1000, fileId=file_id, fields="*").execute()

        r_list = []
        r_list += r.get("revisions")
        while r.get("nextPageToken"):
            print(f"{len(r_list)} total revisions so far...fetching next batch.")
            r = (
                drive_service.revisions()
                .list(
                    pageSize=1000,
                    fileId=file_id,
                    fields="*",
                    pageToken=r.get("nextPageToken"),
                )
                .execute()
            )
            r_list += r.get("revisions")

        return r


# MARK: - GMail
class GMail:
    """
    Functions for interacting with GMail.

    .. image:: ../images_source/gsuite_tools/gmail1.png

    """

    # MARK: - create_service_serv_acct
    @classmethod
    def create_service_serv_acct(
        cls, member_acct_email: str, token_path: Optional[str] = None, scopes: Optional[List[str]] = None
    ) -> Tuple[Any, Any]:
        """
        Creates a GMail authenticated credentials object.

        :param member_acct_email: GSuite service acct email address.
        :param token_path: Path to GSuite authentication token.
        :param scopes: Optional list of OAuth scopes. Defaults to module-level SCOPES.
        :return: Return GMail authenticated credentials object.
        """
        import os

        from apiclient.discovery import build
        from oauth2client.service_account import ServiceAccountCredentials

        token_path = token_path or os.environ.get("GSUITE_TOKEN_PATH")
        _scopes = scopes or ([s.strip() for s in os.environ["GSUITE_SCOPES"].split(",")] if os.environ.get("GSUITE_SCOPES") else SCOPES)
        credentials = ServiceAccountCredentials.from_json_keyfile_name(filename=token_path, scopes=_scopes)

        credentials = credentials.create_delegated(member_acct_email)

        service = build("gmail", "v1", credentials=credentials)
        return service, credentials

    # MARK: - create_service_serv_acct_dict
    @classmethod
    def create_service_serv_acct_dict(cls, member_acct_email: str, dict_creds: Dict[str, Any], scopes: Optional[List[str]] = None) -> Tuple[Any, Any]:
        """
        Creates a GDrive authenticated credentials object.

        :param member_acct_email: GDrive service acct email address.
        :param token_path: Path to GDrive authentication token.
        :param scopes: Optional list of OAuth scopes. Defaults to module-level SCOPES.
        :return: Return GDrive authenticated credentials object.
        """
        import os

        from apiclient.discovery import build
        from oauth2client.service_account import ServiceAccountCredentials

        _scopes = scopes or ([s.strip() for s in os.environ["GSUITE_SCOPES"].split(",")] if os.environ.get("GSUITE_SCOPES") else SCOPES)
        credentials = ServiceAccountCredentials.from_json_keyfile_dict(keyfile_dict=dict_creds, scopes=_scopes)

        credentials = credentials.create_delegated(member_acct_email)

        service = build("gmail", "v1", credentials=credentials)
        return service, credentials

    # MARK: - create_service
    @classmethod
    def create_service(
        cls,
        cred_path: str,
        token_path: Optional[str] = None,
        working_dir: Optional[str] = None,
        scopes: Optional[List[str]] = None,
    ) -> Any:
        """
        Creates an authenticated service object for GMail.

        :param cred_path: Path to Google credentials object.
        :param token_path: Path of authentication token.
        :param working_dir: Path of working directory to store token if token path not specified.
        :return: Authenticated service object for GMail
        """
        import os

        from apiclient.discovery import build
        from google.auth.transport.requests import Request
        from google_auth_oauthlib.flow import InstalledAppFlow

        cred_path = cred_path or os.environ.get("GSUITE_TOKEN_PATH")

        if token_path:
            if os.path.exists(token_path):
                with open(token_path, "rb") as token:
                    creds = pickle.load(token)
        # If there are no (valid) credentials available, let the user log in.
        if not creds or not creds.valid:
            if creds and creds.expired and creds.refresh_token:
                creds.refresh(Request())
            else:
                flow = InstalledAppFlow.from_client_secrets_file(
                    cred_path, scopes or ([s.strip() for s in os.environ["GSUITE_SCOPES"].split(",")] if os.environ.get("GSUITE_SCOPES") else SCOPES)
                )
                creds = flow.run_local_server()
            # Save the credentials for the next run
            with open((working_dir or "") + "token.pickle", "wb") as token:
                pickle.dump(creds, token)

        service = build("gmail", "v1", credentials=creds)
        return service

    # MARK: - send_email
    @classmethod
    def send_email(
        cls,
        service: Any,
        to: List[str],
        sender: str,
        subject: str,
        message_text: str,
        attachments: Any = False,
        attachments_bytes: Any = False,
        attachment_types: Any = False,
        attachment_names: Any = False,
        message_is_html: bool = False,
        req_limit: int = 1,
    ) -> Optional[Any]:
        """
        Sends an email via GMail.

        :param service: Authenticated service object for GMail.
        :param to: Target email address.
        :param sender: Sender email address.
        :param subject: Subject for email.
        :param message_text: Message body text.
        :param attachments: List of message attachments.
        :param message_is_html: Whether or not message_text is in HTML format.
        :return: Sent message.
        """

        message = MIMEText(message_text, "html" if message_is_html else "plain") if not attachments else MIMEMultipart()
        message["to"] = ", ".join(to) if len(to) > 1 else to[0]
        message["from"] = sender
        message["subject"] = subject
        if message_text and attachments:
            message.attach(MIMEText(message_text, "html" if message_is_html else "plain"))

        if attachments:
            msg = MIMEText(message_text, "html" if message_is_html else "plain")

            # if the attachments are passed as bytes
            if attachments_bytes:
                for idx, attch in enumerate(attachments):
                    print(f"Adding attachment from memory: {attch}")

                    main_type = "application"
                    sub_type = attachment_types[idx]

                    msg = MIMEBase(main_type, sub_type)
                    msg.set_payload(attch)
                    encoders.encode_base64(msg)

                    msg.add_header(
                        "Content-Disposition",
                        "attachment",
                        filename=attachment_names[idx],
                    )

                    message.attach(msg)

            else:
                # loop through attachment list
                for idx, attch in enumerate(attachments):
                    print(f"Adding attachment from disk: {attch}")

                    content_type, encoding = mimetypes.guess_type(attch)
                    if content_type is None or encoding is not None:
                        content_type = "application/octet-stream"
                    main_type, sub_type = content_type.split("/", 1)

                    if main_type == "text":
                        fp = open(attch, "rb")
                        msg = MIMEText(fp.read(), _subtype=sub_type)
                        fp.close()

                    elif main_type == "image":
                        fp = open(attch, "rb")
                        msg = MIMEImage(fp.read(), _subtype=sub_type)
                        fp.close()

                    elif main_type == "audio":
                        fp = open(attch, "rb")
                        msg = MIMEAudio(fp.read(), _subtype=sub_type)
                        fp.close()

                    elif main_type == "application":
                        fp = open(attch, "rb")
                        msg = MIMEApplication(fp.read(), _subtype=sub_type)
                        fp.close()

                    else:
                        fp = open(attch, "rb")
                        msg = MIMEBase(main_type, sub_type)
                        msg.set_payload(fp.read())
                        encoders.encode_base64(msg)
                        fp.close()

                    filename = os.path.basename(attch) if not attachment_names else attachment_names[idx]

                    msg.add_header("Content-Disposition", "attachment", filename=filename)

                    message.attach(msg)

        msg = {"raw": (base64.urlsafe_b64encode(message.as_string().encode()).decode())}

        req_count = 0
        while req_count < req_limit:
            try:
                message = service.users().messages().send(userId="me", body=msg).execute()
                return message
            except:
                time.sleep(3)
                req_count += 1

    # MARK: - delete_emails
    @classmethod
    def delete_emails(cls, service: Any, label_ids: Any = False, user_id: str = "me") -> None:
        """
        Deletes emails from a GMAIL inbox
        :param service: Authenticated service object for GMail.
        :param label_ids: GMail label IDs to pull messages for, default="INBOX"
        :param user_id: User ID to pull emails for, default="me"
        :return:
        """
        result = service.users().messages().list(userId=user_id, labelIds=["INBOX"] if not label_ids else label_ids).execute()
        messages = result.get("messages", [])
        if not messages:
            print("No messages found.")
        else:
            print(f"Deleting {len(messages)} messages...")
            for message in messages:
                service.users().messages().delete(userId="me", id=message["id"]).execute()

    # MARK: - get_emails
    @classmethod
    def get_emails(
        cls,
        service: Any,
        label_ids: Any = False,
        custom_tree_branch_list: Any = False,
        user_id: str = "me",
        req_limit: int = 1,
    ) -> Any:
        """
        Returns a Pandas DataFrame of received email details.

        :param service: Authenticated service object for GMail.
        :param user_id: User ID to pull emails for, default="me"
        :param label_ids: GMail label IDs to pull messages for, default="INBOX"
        :param custom_tree_branch_list: List of branch elements to navigate HTML body data (ex: [0,1,0])
        :return: Pandas DataFrame of received email details.
        """
        import pandas as pd

        # get mailbox items
        req_count = 0
        while req_count < req_limit:
            try:
                results = (
                    service.users()
                    .messages()
                    .list(
                        userId=user_id,
                        labelIds=["INBOX"] if not label_ids else label_ids,
                    )
                    .execute()
                )
                break
            except:
                print("Exception...sleeping")
                time.sleep(3)
                req_count += 1

        results_list = []
        results_list.append(results)
        while results.get("nextPageToken"):
            req_count = 0
            while req_count < req_limit:
                try:
                    results = (
                        service.users()
                        .messages()
                        .list(
                            userId=user_id,
                            labelIds=["INBOX"] if not label_ids else label_ids,
                            pageToken=results.get("nextPageToken"),
                        )
                        .execute()
                    )
                    break
                except:
                    print("Exception...sleeping")
                    time.sleep(3)
                    req_count += 1

            results_list.append(results)

        # get messages
        messages_all = []
        for ix, row in enumerate(results_list):
            messages_all += row.get("messages", [])

        # messages = results.get('messages', [])

        # create result data element lists
        mime_types_all = []
        ids_all = []
        text_all = []
        to_all = []
        from_all = []
        dates_all = []
        subject_all = []
        labels_all = []

        # for each message, pull data elements
        for idx, message in enumerate(messages_all):
            req_count = 0
            while req_count < req_limit:
                try:
                    msg = service.users().messages().get(userId=user_id, id=message["id"], format="full").execute()
                    break
                except:
                    print("Exception...sleeping")
                    time.sleep(3)
                    req_count += 1

            # if there are multiple parts of the message get the message body from first part
            if msg.get("payload").get("parts"):
                # if we passed a custom list of branches to follow to get the data
                if custom_tree_branch_list:
                    try:
                        tree_trunk = msg.get("payload")
                        for idxx, branch in enumerate(custom_tree_branch_list):
                            print(branch)
                            tree_trunk = tree_trunk.get("parts")[branch]
                        text = tree_trunk.get("body").get("data")
                    except Exception as e:
                        print(str(e))
                        text = False

                else:
                    # message text
                    text = msg.get("payload").get("parts")[0].get("body").get("data")
                if text:
                    text = base64.urlsafe_b64decode(text.encode("ASCII")).decode("utf-8")
                try:
                    mime_types_all.append([x["mimeType"] for x in msg["payload"]["parts"]])
                except:
                    mime_types_all.append("")
            else:
                mime_types_all.append("")
                text = msg.get("payload").get("body").get("data")
                if text:
                    text = base64.urlsafe_b64decode(text.encode("ASCII")).decode("utf-8")

            # get desired data element keys
            data_keys = [x["name"] for x in msg["payload"]["headers"] if x.get("name") in ["Date", "Subject", "To", "From"]]

            # get desired data element values
            data_vals = [x["value"] for x in msg["payload"]["headers"] if x.get("name") in ["Date", "Subject", "To", "From"]]

            data_dict = dict(zip(data_keys, data_vals))

            # append to lists
            text_all.append(text)
            ids_all.append(msg["id"])
            dates_all.append(data_dict.get("Date"))
            subject_all.append(data_dict.get("Subject"))
            from_all.append(data_dict.get("From"))
            to_all.append(data_dict.get("To"))
            labels_all.append(msg["labelIds"])

        # combine all messages to export
        msg_df = pd.DataFrame(
            {
                "mime_types": mime_types_all,
                "ids": ids_all,
                "text": text_all,
                "to": to_all,
                "from": from_all,
                "dates": dates_all,
                "subject": subject_all,
                "labels": labels_all,
            }
        )

        return msg_df

    # MARK: - get_mailbox_labels
    @classmethod
    def get_mailbox_labels(cls, service: Any, user_id: str = "me", req_limit: int = 1) -> Optional[Any]:
        """

        :param service:
        :param user_id:
        :return:
        """
        req_count = 0
        while req_count < req_limit:
            try:
                results = service.users().labels().list(userId=user_id).execute()
                return results
            except:
                print("Exception...sleeping")
                time.sleep(3)
                req_count += 1

    # MARK: - download_email_attachment
    @classmethod
    def download_email_attachment(
        cls,
        service: Any,
        msg_id: str,
        sav_dir: Any = False,
        user_id: str = "me",
        save_memory: bool = False,
        req_limit: int = 1,
    ) -> List[Dict[str, bytes]]:
        """
        Downloads email attachments for a given emails.

        :param service: Authenticated service object for GMail.
        :param msg_id: ID of message to download attachments for.
        :param sav_dir: Directory to save an attachment into.
        :param user_id: User ID to pull emails for, default="me".
        :return: Downloaded email attachments.
        """

        # get message details
        req_count = 0
        while req_count < req_limit:
            try:
                message = service.users().messages().get(userId=user_id, id=msg_id).execute()
                break
            except:
                print("Exception...sleeping")
                time.sleep(3)
                req_count += 1

        file_data_list = []
        # loop through payload sections
        for part in message["payload"]["parts"]:
            # if there is a filename with an attachment id
            if part["filename"]:
                if "data" in part["body"]:
                    data = part["body"]["data"]
                else:
                    att_id = part["body"]["attachmentId"]
                    req_count = 0
                    while req_count < req_limit:
                        try:
                            att = service.users().messages().attachments().get(userId=user_id, messageId=msg_id, id=att_id).execute()
                            break
                        except:
                            print("Exception...sleeping")
                            time.sleep(3)
                            req_count += 1

                    data = att["data"]
                file_data = base64.urlsafe_b64decode(data.encode("UTF-8"))
                path = part["filename"]
                if save_memory:
                    file_data_list.append({path: file_data})
                else:
                    # save attachment
                    with open(sav_dir + path, "wb") as f:
                        f.write(file_data)

        return file_data_list

    # MARK: - update_message_label
    @classmethod
    def update_message_label(cls, message_id: str, service: Any, data: Dict[str, Any], user_id: str = "me") -> Any:
        result = service.users().messages().modify(id=message_id, userId=user_id, body=data).execute()

        return result


# MARK: - GDocs
class GDocs:
    # MARK: - create_docs_service
    @classmethod
    def create_docs_service(cls, credentials: Any) -> Any:
        from apiclient import discovery
        from httplib2 import Http

        DISCOVERY_DOC = "https://docs.googleapis.com/$discovery/rest?version=v1"

        http = credentials.authorize(Http())
        docs_service = discovery.build("docs", "v1", http=http, discoveryServiceUrl=DISCOVERY_DOC)

        return docs_service

    # MARK: - get_doc
    @classmethod
    def get_doc(cls, doc_id: str, docs_service: Any) -> Any:
        doc = docs_service.documents().get(documentId=doc_id).execute()
        return doc

    # MARK: - read_paragraph_element
    @classmethod
    def read_paragraph_element(cls, element: Dict[str, Any]) -> str:
        """Returns the text in the given ParagraphElement.

        Args:
            element: a ParagraphElement from a Google Doc.
        """
        text_run = element.get("textRun")
        if not text_run:
            return ""
        return text_run.get("content")

    # MARK: - read_structural_elements
    @classmethod
    def read_structural_elements(cls, elements: List[Dict[str, Any]]) -> str:
        """Recurses through a list of Structural Elements to read a document's text where text may be
        in nested elements.

        Args:
            elements: a list of Structural Elements.
        """
        text = ""
        for value in elements:
            if "paragraph" in value:
                elements = value.get("paragraph").get("elements")
                for elem in elements:
                    text += GDocs.read_paragraph_element(elem)
            elif "table" in value:
                # The text in table cells are in nested Structural Elements and tables may be
                # nested.
                table = value.get("table")
                for row in table.get("tableRows"):
                    cells = row.get("tableCells")
                    for cell in cells:
                        text += GDocs.read_structural_elements(cell.get("content"))
            elif "tableOfContents" in value:
                # The text in the TOC is also in a Structural Element.
                toc = value.get("tableOfContents")
                text += GDocs.read_structural_elements(toc.get("content"))
        return text
