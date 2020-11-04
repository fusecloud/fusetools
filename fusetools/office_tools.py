"""
Microsoft Office and Adobe functions..

|pic1| |pic2| |pic3| |pic4|
    .. |pic1| image:: ../images_source/office_tools/excel1.jpeg
        :width: 25%
    .. |pic2| image:: ../images_source/office_tools/outlook1.jpeg
        :width: 25%
    .. |pic3| image:: ../images_source/office_tools/word1.png
        :width: 35%
    .. |pic4| image:: ../images_source/office_tools/pdf1.png
        :width: 20%
"""

import docx
import pandas as pd
import os
import glob
from PyPDF2 import PdfFileWriter, PdfFileReader
from colorama import Fore
from fusetools.transfer_tools import Local
from pathlib import Path


class PDF:
    """
    Functions for interacting with PDFs.

    .. image:: ../images_source/office_tools/pdf1.png
    """

    @classmethod
    def merger(cls, output_path, input_paths):
        """
        Combine multiple PDFs into one.

        :param output_path: Output path of merged PDFs.
        :param input_paths: Input paths of PDFs to merge.
        :return: Combined PDF.
        """
        pdf_writer = PdfFileWriter()

        for path in input_paths:
            pdf_reader = PdfFileReader(path)
            for page in range(pdf_reader.getNumPages()):
                pdf_writer.addPage(pdf_reader.getPage(page))

        with open(output_path, 'wb') as fh:
            pdf_writer.write(fh)


class Excel:
    """
    Functions for interacting with Excel.

    .. image:: ../images_source/office_tools/excel1.jpeg
    """

    @classmethod
    def open_wb(cls, dir):
        """
        Opens a Microsoft Excel workbook.

        :param dir: Path of Excel workbook.
        :return: Opened Excel workbook.
        """

        try:
            import win32com.client
        except:
            pass

        xl = win32com.client.Dispatch("Excel.Application")
        xl.Visible = True
        wb = xl.Workbooks.Open(Filename=dir, ReadOnly=1)
        return wb

    @classmethod
    def run_macro(cls, module):
        """
        Runs a macro in Excel.

        :param module: Name of Macro to run.
        :return: Excel Macro that ran.
        """

        try:
            import win32com.client
        except:
            pass

        xl = win32com.client.Dispatch("Excel.Application")
        xl.Visible = True
        xl.Application.Run(module)

    @classmethod
    def save_close(cls, wb, save_changes=False):
        """
        Closes an Excel workbook.

        :param wb: Excel workbook.
        :param save_changes: Whether or not to save changes when closing the workbook (Default=False).
        :return: Closed Excel workbook.
        """

        try:
            import win32com.client
        except:
            pass

        xl = win32com.client.Dispatch("Excel.Application")
        wb.Close(SaveChanges=1 if save_changes else 0)
        xl.Application.Quit()


class Outlook:
    """
    Functions for interacting with Outlook.

    .. image:: ../images_source/office_tools/outlook1.jpeg
    """

    @classmethod
    def download_attachments(cls, outlook_folder_name,
                             dl_dir,
                             attch_list_file,
                             attch_list_file_sht=False,
                             start_date=False,
                             end_date=False):
        """
        Downloads attachments from an Outlook folder..

        :param outlook_folder_name: Name of Outlook folder to download attachments from.
        :param dl_dir: Directory to download attachments to.
        :param attch_list_file: CSV or Excel file with list of attachment criteria.
        :param attch_list_file_sht: Name of Excel sheet if 'attch_list_file' is an Excel file.
        :param start_date: Minimum received date for email.
        :param end_date: Maximum received date for email.
        :return: Downloaded attachments from Outlook folder.
        """

        # get criteria
        if attch_list_file_sht:
            df = pd.read_excel(attch_list_file, sheet_name=attch_list_file_sht)
        else:
            df = pd.read_csv(attch_list_file)
        # df = df.query("proc == 'wbr'").reset_index(drop=True)
        df['subject'] = df['subject'].str.lower()
        df['name'] = df['name'].str.lower()
        df['attch_str'] = df['attch_str'].str.lower()

        # get df of emails
        outlook_df, inbox = Outlook.outlook_folder_df(folder_name=outlook_folder_name)
        outlook_df['attach_str'] = outlook_df['attach'].str.lower()
        outlook_df['attach_str'] = outlook_df['attach_str'].fillna(value="no attachment")

        # get folder
        for folder in inbox.Folders:
            if str(folder) == outlook_folder_name:
                tgt_folder = folder
                break

        # get prior downloads
        files = Local.get_all_filetimes(dl_dir=dl_dir)
        files['rec_times'] = files['files'].str[:14]

        # for each target attachment, find emails & download files
        for idx, row in df.iterrows():
            attch_q = row['attch_str']
            outlook_df_sub = outlook_df[outlook_df['attach_str'].str.contains(attch_q)]

            # loop thru emails
            for idxx, roww in outlook_df_sub.iterrows():
                tgt_item = tgt_folder.Items[int(idxx)]
                rec_time = outlook_df_sub.loc[idxx, 'rec_time']
                print(f"match found on email: {str(tgt_item)}, received at {rec_time}")
                if start_date:
                    if int(rec_time[:8]) >= int(start_date):
                        if end_date:
                            if int(rec_time[:8]) <= int(end_date):
                                print(Fore.GREEN + "rec date is inside start date...saving")
                            else:
                                print(Fore.RED + "rec date falls outside end date...skipping")
                                continue
                        else:
                            print(Fore.GREEN + "rec date is >= than start date and <= end date...saving")
                    else:
                        print(Fore.RED + "rec date is < than start date...skipping")
                        continue

                # loop through attachments
                for attch in tgt_item.Attachments:
                    if f"{rec_time}_{str(attch)}" in files['files'].to_list():
                        print(Fore.YELLOW + f"skipping: '{str(attch)}' in email: {str(tgt_item)}...already downloaded")
                    else:
                        print(Fore.GREEN + f"downloading: '{str(attch)}' in email: {str(tgt_item)}")
                        try:
                            attch.SaveAsFile(f"{dl_dir}{rec_time}_{str(attch)}")
                        except:
                            attch.SaveAsFile(f"{Path(dl_dir + '/' + rec_time + '_' + str(attch))}.xlsx")

    @classmethod
    def outlook_folder_df(cls, folder_name):
        """
        Downloads an Outlook folder's emails details into a Pandas DataFrame.

        :param folder_name: Name of Outlook folder to download.
        :return: Pandas DataFrame of Outlook folder's email details.
        """

        try:
            import win32com.client
        except:
            pass

        # get outlook
        outlook = win32com.client.Dispatch("Outlook.Application").GetNamespace("MAPI")
        # get account
        accounts = win32com.client.Dispatch("Outlook.Application").Session.Accounts
        account = accounts[0]
        # get trgt folder
        inbox = outlook.Folders(account.DeliveryStore.DisplayName)
        for folder in inbox.Folders:
            if str(folder) == folder_name:
                tgt_folder = folder
                break

        print(Fore.RED + f"scanning {len(tgt_folder.Items)} emails...")

        # get each external
        for idx, item in enumerate(tgt_folder.Items):

            if idx % 10 == 0:
                print(Fore.BLUE + f"progress...{idx / len(tgt_folder.Items)}%")

            emails = []
            subjects = []
            senders = []
            body = []
            rec_time = []
            attachments = []
            try:
                emails.append(str(item))
            except:
                continue
            try:
                subjects.append(str(item.Subject))
            except:
                subjects.append("NA")
            try:
                senders.append(str(item.Sender))
            except:
                senders.append("NA")
            try:
                body.append(str(item.body))
            except:
                body.append("NA")
            try:
                rec_time.append(item.ReceivedTime.strftime('%Y%m%d%H%M%S'))
            except:
                rec_time.append("NA")

            # get all attachments
            for attch in item.Attachments:
                attachments.append(str(attch))

            # make df of metadata
            item_df = pd.DataFrame({
                "external": emails,
                "subject": subjects,
                "sender": senders,
                "body": body,
                "rec_time": rec_time,
                "attach": str(attachments)
            }).assign(key=1)

            # combine all
            if idx == 0:
                item_df_all = item_df
            else:
                item_df_all = pd.concat([
                    item_df_all,
                    item_df
                ])

            item_df_all.reset_index(drop=True, inplace=True)

        return item_df_all, inbox

    @classmethod
    def get_outlook_appts(cls, sub_str=False):
        """
        Download Outlook appointment details into a Pandas DataFrame.

        :param sub_str:
        :return:
        """
        try:
            import win32com.client
        except:
            pass

        outlook = win32com.client.Dispatch("Outlook.Application").GetNamespace("MAPI")

        # calendar folder
        calendar = outlook.GetDefaultFolder(9)
        OUTLOOK_FORMAT = '%Y-%m-%d %H:%M'

        appointments = calendar.Items
        subjects = []
        times = []
        for appointment in appointments:
            subjects.append((appointment.Subject))
            times.append((appointment.Start.Format(OUTLOOK_FORMAT)))

        df = pd.DataFrame({
            "subject": subjects,
            "time": times
        })

        if sub_str:
            df = df[df['subject'].str.lower().str.contains(sub_str)]

        return df

    @classmethod
    def send_email(cls, send_to, subject, body, html_body=False, attachment=None):
        """

        :param send_to:
        :param subject:
        :param body:
        :param html_body:
        :param attachment:
        :return:
        """

        try:
            import win32com.client
        except:
            pass

        outlook = win32com.client.Dispatch('outlook.application')
        mail = outlook.CreateItem(0)
        mail.To = send_to
        mail.Subject = subject
        mail.Body = body
        if html_body:
            mail.HTMLBody = html_body

        # To attach a file to the external (optional):
        if attachment != None:
            if isinstance(attachment, type(['list'])):
                for attch in attachment:
                    print(f"\t...Attaching {attch}")
                    mail.Attachments.Add(attch)

            elif isinstance(attachment, type('string')):
                print(f"\t...Attaching {attachment}")
                mail.Attachments.Add(attachment)

            else:
                print("unsupported attatchment list. send string or list.\n    ignoring attachments")

        try:
            mail.Send()
        except Exception as e:
            print(
                "Error occurred. If the error was 'Outlook does not recognize one or more names.' and you're sending to multiple external addresses, use a semicolon ';' to separate external addresses.\n\n")
            raise

    @classmethod
    def send_meeting(cls, subject, start_date, duration, location, sender, recipients):
        """

        :param subject:
        :param start_date:
        :param duration:
        :param location:
        :param sender:
        :param recipients:
        :return:
        """

        try:
            import win32com.client
        except:
            pass

        outlook = win32com.client.Dispatch('outlook.application')
        # CreateItem: 1 -- Outlook Appointment Item
        appt = outlook.CreateItem(1)

        # set the parameters of the meeting
        appt.Start = start_date
        appt.Duration = duration
        appt.Location = location
        appt.Subject = subject

        appt.MeetingStatus = 1  # this enables adding of recipients

        for idxx, rec in enumerate(recipients):
            appt.Recipients.Add(rec)
            print(f"added {rec}")

        appt.Organizer = sender
        appt.ReminderMinutesBeforeStart = 15
        appt.ResponseRequested = True
        appt.Save()
        appt.Send()

    @classmethod
    def delete_outlook_meetings(cls, df_del):
        """

        :param df_del:
        :return:
        """

        try:
            import win32com.client
        except:
            pass

        outlook = win32com.client.Dispatch("Outlook.Application").GetNamespace("MAPI")

        # calendar folder
        calendar = outlook.GetDefaultFolder(9)
        OUTLOOK_FORMAT = '%Y-%m-%d %H:%M'

        # send items to recycle bin
        appointments = calendar.Items
        for idx, appointment in enumerate(appointments):
            if df_del['subject'].str.contains(str(appointment.Subject), regex=False).any():
                print(f"Deleting meeting: {str(appointment.Subject)}")
                appointment.Delete()

        # delete items from recycle bin
        calendar = outlook.GetDefaultFolder(3)
        appointments = calendar.Items
        for idx, appointment in enumerate(appointments):
            appointment.Delete()


class Word:
    """
    Functions for interacting with Outlook.

    .. image:: ../images_source/office_tools/word1.png
    """

    @classmethod
    def list_to_word(cls, value_list, export_dir, doc_name):
        """
        Converts a list of strings to a Microsoft Word document.

        :param value_list: List of strings to convert.
        :param export_dir: Filepath to export Word document to.
        :param doc_name: Name for saved Word document.
        :return: Microsoft Word document contains list of strings.
        """
        doc = docx.Document()
        for idx, row in enumerate(value_list):
            print(row)
            doc.add_paragraph(row)

        doc.save(export_dir + doc_name)
