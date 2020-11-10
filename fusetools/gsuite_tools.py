"""
Google Suite Tools.

|pic1|
    .. |pic1| image:: ../images_source/gsuite_tools/gsuitelogo1.png
        :width: 45%
"""
import json
from email.mime.text import MIMEText
import base64
import logging
import mimetypes
import os
import pickle
import smtplib
import time
import urllib
import html
# import pyOpenSSL
from email import encoders
from email.headerregistry import Address
from email.message import EmailMessage
from email.mime.audio import MIMEAudio
from email.mime.base import MIMEBase
from email.mime.image import MIMEImage
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.utils import make_msgid
from os.path import splitext
from pathlib import Path
import email
from email.policy import default
import httplib2
import pandas as pd
from apiclient import discovery
from apiclient.discovery import build
import io
from google.auth.transport.requests import Request
from google.oauth2.service_account import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.http import MediaFileUpload, MediaIoBaseDownload
from oauth2client import file, client, tools
from oauth2client.service_account import ServiceAccountCredentials

SCOPES = ["https://www.googleapis.com/auth/spreadsheets",
          "https://www.googleapis.com/auth/drive.file",
          "https://www.googleapis.com/auth/drive",
          'https://www.googleapis.com/auth/gmail.send',
          'https://www.googleapis.com/auth/gmail.readonly'
          ]


class GSheets:
    """
    Functions for interacting with Google Sheets.

    .. image:: ../images_source/gsuite_tools/googlesheets1.png

    """

    @classmethod
    def create_service_serv_acct(cls, member_acct_email, token_path):

        credentials = ServiceAccountCredentials.from_json_keyfile_name(
            filename=token_path,
            scopes=SCOPES)

        credentials = (
            credentials
                .create_delegated(member_acct_email)
        )

        service = build('sheets', 'v4', credentials=credentials)
        return service

    @classmethod
    def make_google_sheet(cls, ss_name, credentials):
        """
        Creates a Google Sheet in one's GSuite account.

        :param ss_name: Name of the Google Sheet to be created.
        :param credentials: GSuite credentials object.
        :return: Id of newly created Google Sheet.
        """
        SHEETS = discovery.build("sheets", "v4", credentials=credentials)
        data = {
            "properties": {"title": ss_name}
        }

        try:
            sheet = SHEETS.spreadsheets().create(body=data).execute()
        except:
            print("Exception...sleeping")
            time.sleep(3)
            sheet = SHEETS.spreadsheets().create(body=data).execute()

        sheet_id = sheet.get("spreadsheetId")
        print(f"Created wb: {ss_name}")
        return sheet_id

    @classmethod
    def add_google_sheet_tab(cls, spreadsheet_id, tab_name, credentials):
        """
        Adds a tab to a Google Sheet.

        :param spreadsheet_id: Id of Google Sheet to add tab to.
        :param tab_name: Name of the tab to be added.
        :param credentials: GSuite credentials object.
        :return: Result object for API call.
        """
        data = {'requests': [{'addSheet': {'properties': {'title': tab_name}}}]}
        service = build('sheets', 'v4', credentials=credentials)

        try:
            res = service.spreadsheets().batchUpdate(
                spreadsheetId=spreadsheet_id,
                body=data,
            ).execute()

        except:

            print("Exception...sleeping")
            time.sleep(3)
            res = service.spreadsheets().batchUpdate(
                spreadsheetId=spreadsheet_id,
                body=data,
            ).execute()

        print(f"Added sheet: {tab_name}")

        return res

    @classmethod
    def get_google_sheet(cls, spreadsheet_id, range_name, credentials, tab_name=False):
        """
        Gets data from a Google Sheet.

        :param spreadsheet_id: Id of Google Sheet to retrieve.
        :param range_name: Row/Column of Sheet range to retrieve (ex: A1:A99).
        :param tab_name: Name of tab to pull data from (optional).
        :param credentials: GSuite credentials object.
        :return: Pandas DataFrame of retrieved data.
        """

        service = build('sheets', 'v4', credentials=credentials)

        # Call the Sheets API
        sheet = service.spreadsheets()
        if tab_name:
            gsheet = sheet.values().get(spreadsheetId=spreadsheet_id,
                                        range=tab_name + "!" + range_name).execute()

        else:
            gsheet = sheet.values().get(spreadsheetId=spreadsheet_id,
                                        range=range_name).execute()

        header = gsheet.get("values")[0]
        values = gsheet.get('values')[1:]

        df = pd.DataFrame(values)
        df = df[df.columns[:len(header)]]
        df.columns = header

        return df

    @classmethod
    def freeze_rows_google_sheet(cls, spreadsheet_id, tab_id, freeze_row, credentials):
        """
        Freezes the rows of a given Google Sheet.

        :param spreadsheet_id: Id of Google Sheet to retrieve.
        :param tab_id: Id of tab to modify.
        :param freeze_row: Spreadsheet row to freeze.
        :param credentials: GSuite credentials object.
        :return: Result object for API call.
        """
        service = build('sheets', 'v4', credentials=credentials)

        data = {
            "requests": [
                {
                    "updateSheetProperties": {
                        "properties": {
                            "sheetId": tab_id,
                            "gridProperties": {
                                "frozenRowCount": freeze_row
                            }
                        },
                        "fields": "gridProperties.frozenRowCount"
                    }
                }
            ]
        }

        try:
            res = service.spreadsheets().batchUpdate(
                spreadsheetId=spreadsheet_id,
                body=data,
            ).execute()
        except:
            print("Exception....sleeping")
            time.sleep(3)
            res = service.spreadsheets().batchUpdate(
                spreadsheetId=spreadsheet_id,
                body=data,
            ).execute()

        return res

    @classmethod
    def update_google_sheet_val(cls, spreadsheet_id, tab_id, val, row, col, credentials):
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
        service = build('sheets', 'v4', credentials=credentials)

        data = {"requests": {
            "updateCells": {
                "rows": [
                    {
                        "values": [{
                            "userEnteredValue": {
                                "formulaValue": val
                            }
                        }]
                    }
                ],
                "fields": "userEnteredValue",
                "start": {
                    "sheetId": tab_id,
                    "rowIndex": row,
                    "columnIndex": col
                }
            }
        }
        }

        try:
            res = (
                service
                    .spreadsheets()
                    .batchUpdate(spreadsheetId=spreadsheet_id,
                                 body=data)
            ).execute()
        except:
            print("Exception....sleeping")
            time.sleep(3)
            res = (
                service
                    .spreadsheets()
                    .batchUpdate(spreadsheetId=spreadsheet_id,
                                 body=data)
            ).execute()

        return res

    @classmethod
    def update_google_sheet_df(cls, spreadsheet_id, df, data_range, credentials):
        """
        Uploads a Pandas DataFrame to a Google Sheet.

        :param spreadsheet_id: ID of spreadsheet to update.
        :param df: Pandas DataFrame to update spreadsheet with.
        :param data_range: Spreadsheet range to insert DataFrame into.
        :param credentials: GSuite credentials object.
        :return: API response.
        """
        service = build('sheets', 'v4', credentials=credentials)
        value_input_option = 'USER_ENTERED'
        insert_data_option = 'INSERT_ROWS'

        write_df = []

        for idx, row in df.iterrows():
            write_df.append(df.iloc[idx].tolist())

        write_df = [df.columns.tolist()] + write_df

        body = dict({"values": write_df})

        try:
            res = service.spreadsheets().values().append(
                spreadsheetId=spreadsheet_id, range=data_range,
                valueInputOption=value_input_option,
                # insertDataOption=insert_data_option,
                body=body).execute()
        except:
            print("Exception...sleeping")
            time.sleep(3)
            res = service.spreadsheets().values().append(
                spreadsheetId=spreadsheet_id, range=range,
                valueInputOption=value_input_option,
                # insertDataOption=insert_data_option,
                body=body).execute()

        return res

    @classmethod
    def get_google_sheet_tabs(cls, spreadsheet_id, credentials):
        """
        Get the names and IDs of tabs for a given Google Sheet.

        :param spreadsheet_id: ID of spreadsheet to update.
        :param credentials: GSuite credentials object.
        :return: Names and IDs of tabs for a given Google Sheet.
        """
        service = build('sheets', 'v4', credentials=credentials)
        sheet_metadata = service.spreadsheets().get(spreadsheetId=spreadsheet_id).execute()
        tab_names = []
        tab_ids = []
        for idx, e in enumerate(sheet_metadata.get("sheets")):
            tab_names.append(e.get("properties").get("title"))
            tab_ids.append(e.get("properties").get("sheetId"))

        return tab_names, tab_ids


class GDrive:
    """
    Functions for interacting with Google Drive.

    .. image:: ../images_source/gsuite_tools/googledrive1.jpeg

    """

    @classmethod
    def create_service_serv_acct(cls, member_acct_email, token_path):

        credentials = ServiceAccountCredentials.from_json_keyfile_name(
            filename=token_path,
            scopes=SCOPES)

        credentials = (
            credentials
                .create_delegated(member_acct_email)
        )

        service = build('drive', 'v3', credentials=credentials)
        return service


    @classmethod
    def authorize_credentials(cls, cred_path, token_path):
        """
        Creates an authorized credentials object for Google Drive.

        :param cred_path: Local path to GSuite credentials object
        :param token_path: Local path to GSuite authorization token
        :return: Authorized credentials object for GSuite
        """

        creds = None
        if os.path.exists(token_path):
            with open(token_path, 'rb') as token:
                creds = pickle.load(token)
            # If there are no (valid) credentials available, let the user log in.
        if not creds or not creds.valid:
            if creds and creds.expired and creds.refresh_token:
                creds.refresh(Request())
            else:
                flow = InstalledAppFlow.from_client_secrets_file(
                    cred_path, SCOPES)
                creds = flow.run_local_server(port=0)
                # Save the credentials for the next run
            with open(token_path, 'wb') as token:
                pickle.dump(creds, token)
        return creds

    @classmethod
    def download_file(cls, file_id, credentials):
        """
        Downloads a file from a Google Drive account.

        :param file_id: ID for Google Drive file
        :param credentials: GSuite credentials object.
        :return:
        """
        drive_service = build('drive', 'v3', credentials=credentials)
        request = drive_service.files().get_media(fileId=file_id)
        fh = io.BytesIO()
        downloader = MediaIoBaseDownload(fh, request)
        done = False
        while done is False:
            status, done = downloader.next_chunk()
            print("Download %d%%." % int(status.progress() * 100))

    @classmethod
    def get_all_google_items(cls, credentials, folder_id=False):
        """
        Get all files in a Google Drive (or folder if specified).

        :param credentials: GSuite credentials object.
        :param folder_id: GDrive folder to search in (optional)
        :return: Pandas DataFrame of files in a Google Drive.
        """

        DRIVE = discovery.build('drive', 'v3', credentials=credentials)

        if folder_id:
            res = DRIVE.files().list(
                q="'" + folder_id + "' in parents",
                pageSize=100,
                fields="nextPageToken, files(id, name)").execute()
            df = pd.DataFrame(res.get("files"))

        else:
            res = DRIVE.files().list().execute()
            df = pd.DataFrame(res.get("files"))

        return df

    @classmethod
    def create_upload_folder(cls, folder_name, credentials, overwrite_folder=False, parent_id=None):
        """
        Creates a folder in Google Drive.

        :param folder_name: Name of folder to create.
        :param credentials: GSuite credentials object.
        :param overwrite_folder: Whether or not to delete and re-create the folder.
        :param parent_id: Id of parent folder (optional).
        :return: Created folder information.
        """
        # Create a folder on Drive, returns the newly created folders ID
        drive_service = build('drive', 'v3', credentials=credentials)

        response = (
            drive_service
                .files()
                .list(
                q="mimeType = 'application/vnd.google-apps.folder'",
                fields='nextPageToken, files(id, name)'
            )
                .execute()
        )

        if overwrite_folder:
            for file in response.get('files', []):

                if folder_name == file.get('name'):
                    print('Overwriting file: %s (%s)' % (file.get('name'), file.get('id')))
                    drive_service.files().delete(fileId=file.get('id')).execute()
                    break

                page_token = response.get('nextPageToken', None)

                if page_token is None:
                    break

        body = {
            'name': folder_name,
            'mimeType': "application/vnd.google-apps.folder"
        }
        if parent_id:
            body['parents'] = [parent_id]
        folder = drive_service.files().create(body=body).execute()

        return folder

    @classmethod
    def upload_files(cls, folder_id, upload_filepaths, credentials, upload_filenames=False):
        """
        Uploads files to a folder on Google Drive.

        :param folder_id: ID of folder to upload files into.
        :param credentials: GSuite credentials object.
        :param upload_filepaths: Filepaths for files to upload.
        :param upload_filenames: Filenames for files, must match length of filepaths (optional). Otherwise uses filepath names.
        :return: Confirmation of files being uploaded.
        """
        drive_service = build('drive', 'v3', credentials=credentials)

        files = []
        for idx, file in enumerate(upload_filepaths):
            name = os.fsdecode(file)
            filename, file_extension = splitext(name, )
            if upload_filenames:
                filename, file_extension = splitext(upload_filenames[idx], )

            # setting mimeType for each file
            # more types available here:
            # https://developer.mozilla.org/en-US/docs/Web/HTTP/Basics_of_HTTP/MIME_types/Common_types

            if file_extension == '.pdf':
                mimeType = 'application/pdf'

            elif '.gz' in file_extension:
                mimeType = "application/gzip"

            elif '.zip' in file_extension:
                mimeType = "application/zip"

            elif file_extension == '.doc':
                mimeType = "application/msword"

            elif file_extension == '.docx':
                mimeType = "application/vnd.openxmlformats-officedocument.wordprocessingml.document"

            elif file_extension == '.ppt':
                mimeType = "application/vnd.ms-powerpoint"

            elif file_extension == '.pptx':
                mimeType = "application/vnd.openxmlformats-officedocument.presentationml.presentation"

            elif '.csv' in file_extension:
                mimeType = "text/csv"

            elif '.txt' in file_extension:
                mimeType = "text/plain"

            elif file_extension == '.xls':
                mimeType = "application/vnd.ms-excel"

            elif file_extension == '.xlsx':
                mimeType = "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"

            elif '.jpg' in file_extension or '.jpeg' in file_extension:
                mimeType = 'image/jpeg'

            elif '.png' in file_extension:
                mimeType = 'image/png'

            elif '.tif' in file_extension or '.tiff' in file_extension:
                mimeType = 'image/tiff'

            elif '.gif' in file_extension:
                mimeType = 'image/gif'

            elif '.json' in file_extension:
                mimeType = 'application/json'

            elif '.mp3' in file_extension:
                mimeType = 'audio/mpeg'

            elif '.mpeg' in file_extension:
                mimeType = 'video/mpeg'

            elif '.wav' in file_extension:
                mimeType = 'video/wav'

            else:
                print(f"File type for {file} not mapped...")
                continue

            file_metadata = {
                'name': filename + file_extension,
                'mimeType': mimeType,
                'parents': [folder_id]
            }

            try:
                media = MediaFileUpload(file, resumable=True)

                file_ = (
                    drive_service
                        .files()
                        .create(
                        body=file_metadata,
                        media_body=media,
                        fields='id')
                        .execute())

                print(f'''file upload success for: {file}''')
                files.append(file_)
            except Exception as e:
                print(str(e))
                print(f'''file upload failed for: {file}''')
                continue

        return files


class GMail:
    """
    Functions for interacting with GMail.

    .. image:: ../images_source/gsuite_tools/gmail1.png

    """

    @classmethod
    def create_service_serv_acct(cls, member_acct_email, token_path):

        credentials = ServiceAccountCredentials.from_json_keyfile_name(
            filename=token_path,
            scopes=SCOPES)

        credentials = (
            credentials
                .create_delegated(member_acct_email)
        )

        service = build('gmail', 'v1', credentials=credentials)
        return service

    @classmethod
    def create_service(cls, cred_path, token_path=None, working_dir=None):
        """
        Creates an authenticated service object for GMail.

        :param cred_path: Path to Google credentials object.
        :param token_path: Path of authentication token.
        :param working_dir: Path of working directory to store token if token path not specified.
        :return: Authenticated service object for GMail
        """

        if token_path:
            if os.path.exists(token_path):
                with open(token_path, 'rb') as token:
                    creds = pickle.load(token)
        # If there are no (valid) credentials available, let the user log in.
        if not creds or not creds.valid:
            if creds and creds.expired and creds.refresh_token:
                creds.refresh(Request())
            else:
                flow = InstalledAppFlow.from_client_secrets_file(
                    cred_path, SCOPES)
                creds = flow.run_local_server()
            # Save the credentials for the next run
            with open(working_dir + 'token.pickle', 'wb') as token:
                pickle.dump(creds, token)

        service = build('gmail', 'v1', credentials=creds)
        return service

    @classmethod
    def send_email(cls, service, to, sender, subject, message_text, attachments=False, message_is_html=False):
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

        message = MIMEText(
            message_text, "html" if message_is_html else "plain"
        ) if not attachments else MIMEMultipart()
        message['to'] = ", ".join(to) if len(to) > 1 else to[0]
        message['from'] = sender
        message['subject'] = subject

        if attachments:
            msg = MIMEText(
                message_text, "html" if message_is_html else "plain"
            )
            # message.attach(msg)

            # loop through attachment list
            for idx, attch in enumerate(attachments):
                content_type, encoding = mimetypes.guess_type(attch)

                if content_type is None or encoding is not None:
                    content_type = 'application/octet-stream'
                main_type, sub_type = content_type.split('/', 1)

                if main_type == 'text':
                    fp = open(attch, 'rb')
                    msg = MIMEText(fp.read(), _subtype=sub_type)
                    fp.close()

                elif main_type == 'image':
                    fp = open(attch, 'rb')
                    msg = MIMEImage(fp.read(), _subtype=sub_type)
                    fp.close()

                elif main_type == 'audio':
                    fp = open(attch, 'rb')
                    msg = MIMEAudio(fp.read(), _subtype=sub_type)
                    fp.close()

                else:
                    fp = open(attch, 'rb')
                    msg = MIMEBase(main_type, sub_type)
                    msg.set_payload(fp.read())
                    encoders.encode_base64(msg)
                    fp.close()

                filename = os.path.basename(attch)
                msg.add_header('Content-Disposition',
                               'attachment',
                               filename=filename)

                message.attach(msg)

        msg = {
            'raw': (
                base64.urlsafe_b64encode(
                    message.as_string()
                        .encode())
                    .decode())}

        message = (
            service
                .users()
                .messages()
                .send(userId="me", body=msg)
                .execute()
        )

        return message

    @classmethod
    def get_emails(cls, service, label_ids=False, user_id="me"):
        """
        Returns a Pandas DataFrame of received email details.

        :param service: Authenticated service object for GMail.
        :param user_id: User ID to pull emails for, default="me"
        :param label_ids: GMail label IDs to pull messages for, default="INBOX"
        :return: Pandas DataFrame of received email details.
        """
        # get mailbox items
        results = (service.users()
                   .messages()
                   .list(userId=user_id,
                         labelIds=['INBOX'] if not label_ids else label_ids).execute()
                   )
        # get messages
        messages = results.get('messages', [])

        # create result data element lists
        mime_types_all = []
        ids_all = []
        text_all = []
        to_all = []
        from_all = []
        dates_all = []
        subject_all = []

        # for each message, pull data elements
        for idx, message in enumerate(messages):
            msg = (service
                   .users()
                   .messages()
                   .get(userId=user_id,
                        id=message['id'],
                        format="full"
                        )
                   .execute()
                   )

            # if there are multiple parts of the message get the message body from first part
            if msg.get("payload").get("parts"):
                # message text
                text = (msg
                        .get("payload")
                        .get("parts")[0]
                        .get("body")
                        .get("data"))
                if text:
                    text = (
                        base64.urlsafe_b64decode(
                            text.encode("ASCII"))
                            .decode("utf-8"))
                try:
                    mime_types_all.append(
                        [x['mimeType']
                         for x in msg['payload']['parts']])
                except:
                    mime_types_all.append("")
            else:
                mime_types_all.append("")
                text = (msg
                        .get("payload")
                        .get("body")
                        .get("data")
                        )
                if text:
                    text = (
                        base64.urlsafe_b64decode(
                            text.encode("ASCII"))
                            .decode("utf-8"))

            # get desired data element keys
            data_keys = \
                [x['name'] for x in msg['payload']['headers']
                 if x.get("name") in ['Date', 'Subject', 'To', 'From']]

            # get desired data element values
            data_vals = \
                [x['value'] for x in msg['payload']['headers']
                 if x.get("name") in ['Date', 'Subject', 'To', 'From']]

            data_dict = dict(zip(data_keys, data_vals))

            # append to lists
            text_all.append(text)
            ids_all.append(msg['id'])
            dates_all.append(data_dict.get("Date"))
            subject_all.append(data_dict.get("Subject"))
            from_all.append(data_dict.get("From"))
            to_all.append(data_dict.get("To"))

        # combine all messages to export
        msg_df = \
            pd.DataFrame({
                "mime_types": mime_types_all,
                "ids": ids_all,
                "text": text_all,
                "to": to_all,
                "from": from_all,
                "dates": dates_all,
                "subject": subject_all
            })

        return msg_df

    @classmethod
    def download_email_attachment(cls, service, msg_id, sav_dir, user_id="me"):
        """
        Downloads email attachments for a given emails.

        :param service: Authenticated service object for GMail.
        :param msg_id: ID of message to download attachments for.
        :param sav_dir: Directory to save an attachment into.
        :param user_id: User ID to pull emails for, default="me".
        :return: Downloaded email attachments.
        """

        # get message details
        message = service.users().messages().get(userId=user_id, id=msg_id).execute()

        # loop through payload sections
        for part in message['payload']['parts']:
            # if there is a filename with an attachment id
            if part['filename']:
                if 'data' in part['body']:
                    data = part['body']['data']
                else:
                    att_id = part['body']['attachmentId']
                    att = (service
                           .users()
                           .messages()
                           .attachments()
                           .get(userId=user_id,
                                messageId=msg_id,
                                id=att_id
                                ).execute()
                           )
                    data = att['data']
                file_data = (base64.urlsafe_b64decode(data.encode('UTF-8')))
                path = part['filename']

                # save attachment
                with open(sav_dir + path, 'wb') as f:
                    f.write(file_data)
