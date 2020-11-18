"""
Transfer applications.

|pic1|
    .. |pic1| image:: ../images_source/transfer_tools/transfer.png
        :width: 30%

"""

import os
import sys
from subprocess import Popen, PIPE
from pathlib import Path
import pandas as pd
import pexpect
import requests
import zipfile
from selenium.webdriver.chrome import webdriver


class Access:
    """
    Functions for accessing file systems and protocols.

    """

    @classmethod
    def proxies(cls, domain):
        """
        A function to create an http/https proxy address.

        :param domain: domain address.
        :return: Http/https proxy address.
        """
        res = {
            'http': 'http://' + \
                    os.environ['usr'] + \
                    ':' + os.environ['pwd'] + \
                    f'@proxyfarm.{domain}.com:8080'
            ,
            'https': 'https://' + \
                     os.environ['usr'] + \
                     ':' + os.environ['pwd'] + \
                     f'@proxyfarm.{domain}.com:8080'
        }

        return res


class Local:
    """
    Functions for accessing local files.

    """

    @classmethod
    def zip_unzip(cls, zip_file_name, file_paths, method="zip"):

        if method == "zip":
            zf = zipfile.ZipFile(zip_file_name, "w")
            for file in file_paths:
                zf.write(
                    os.path.join(
                        "/".join(file.split("/")[:-1]),
                        file.split("/")[-1]
                    ),
                    arcname=file.split("/")[-1])
            zf.close()
        # todo: zip while maintaining folder structure
        # todo: unzip

        pass

    @classmethod
    def clear_delete_directory(cls, directory, method="delete"):
        directory = Path(directory)
        for item in directory.iterdir():
            if item.is_dir():
                Local.clear_delete_directory(item)
            else:
                item.unlink()

        if method == "delete":
            directory.rmdir()

    @classmethod
    def fix_user_path(cls, dir):
        """
        Fixes a local filepath.

        :param dir: Directory to patch.
        :return: Patches directory.
        """

        dir_components = dir.split("/")
        search = "Users"
        for idx, elem in enumerate(dir_components):
            if elem == search:
                break

        dir_components[idx + 1] = os.environ['os_name']
        r = "/".join(dir_components)

        return r

    @classmethod
    def get_all_filetimes(cls, dir, exclude=False):
        """
        Creates a Pandas DataFrame of filenames and file times in a given directory.

        :param dir: Directory of files.
        :param exclude: A string to search for files to exclude.
        :return: Pandas DataFrame of filenames and file times to a directory.
        """

        files = os.listdir(dir)

        if exclude:
            files = [f for f in files if exclude not in f]

        times = [os.path.getmtime(dir + f) for f in files]
        file_times = pd.DataFrame({"files": files, "times": times})
        return file_times

    @classmethod
    def get_latest_file(cls, name, dir, exclude=False):
        """
        Get the latest file in a directory.

        :param name: String match name of file(s)
        :param dir: Directory for the file search
        :param exclude: A string to search for files to exclude.
        :return: Name of most recent file.
        """

        # file name str to lowercase
        name = name.lower()

        # get list of files
        files = os.listdir(dir)

        if exclude:
            files = [f for f in files if exclude not in f]

        times = [os.path.getmtime(dir + f) for f in files]
        file_times = pd.DataFrame({"files": files, "times": times})
        file_times['files_lower'] = file_times['files'].str.lower()
        file_times = file_times[file_times['files_lower'].str.contains(name)]
        read_file = file_times[file_times['times'] == max(file_times['times'])]['files']
        read_file = read_file.values[0]

        return read_file

    @classmethod
    def read_files_like(cls, name, dir, ext_typ, sheet_name=False):
        """
        Reads and concatenates files in a directory that match a string. Returns a Pandas DataFrame.

        :param name: A string search to match.
        :param dir: Directory to search for files in.
        :param ext_typ: Extension type to search for (.XLSX OR .CSV)
        :param sheet_name: If extension type is not ".CSV", specifies the sheet number or sheet name to read.
        :return: Concatenated Pandas DataFrames that match a string.
        """

        files = os.listdir(dir)
        files = pd.DataFrame({"files": files})
        files['files'] = files['files'].str.lower()
        files = files[(
                files['files'].str.contains(name)
                &
                files['files'].str.contains(ext_typ)
        )]
        files.reset_index(inplace=True)

        for idx, f in files.iterrows():
            if ext_typ == "csv":
                dat = pd.read_csv(dir + f['files'])
            else:
                dat = pd.read_excel(dir + f['files'], sheet_name=sheet_name)
            if idx == 0:
                dat_all = dat
            else:
                dat_all = pd.concat([dat_all, dat])

        return dat_all


class SSH:
    """
    Functions for transferring data with SSH.

    """

    @classmethod
    def ssh_transfer_files(cls, local_file_path,
                           target_file_path,
                           target_user,
                           target_ip,
                           target_pwd,
                           direction="send"
                           ):

        if direction == "send":
            cmd = f"scp -rp {local_file_path} {target_user}@{target_ip}:{target_file_path}"
            child = pexpect.spawn(cmd, encoding='utf-8')
            child.logfile = sys.stdout
            child.expect(".*password:", timeout=None)
            child.sendline(target_pwd)
            child.expect(pexpect.EOF, timeout=None)
        else:
            cmd = f"scp -r {target_user}@{target_ip}:{target_file_path} {local_file_path}"
            child = pexpect.spawn(cmd, encoding='utf-8')
            child.logfile = sys.stdout
            child.expect(".*password:", timeout=None)
            child.sendline(target_pwd)
            child.expect(pexpect.EOF, timeout=None)


class SFTP:
    """
    Functions for using SFTP/FTP protocols.

    """

    @classmethod
    def winscp_sftp_connect(cls,
                            action,
                            exe_path,
                            conn_str,
                            hostkey,
                            src_path,
                            dest_path,
                            file_name,
                            new_file_name=None,
                            match_all=False):
        """
        Connects to an SFTP server.

        :param action: Actions allowed are "put" or "get".
        :param exe_path: Path where WinSCP executable is installed on machine.
        :param conn_str: Connection string to connect to ftp site.
        :param hostkey: HostKey credential required to match.
        :param src_path: To get file from scp site use "/", to get file from machine use "\\".
        :param dest_path: To put file on scp site use "/", to put file on machine use "\\".
        :param file_name: Use complete file name with file format (".csv") to get specific file, exclude format to get multiple matches.
        :param new_file_name: Include file format.
        :param match_all: Whether or not to match on.
        :return: If 'get' then a downloaded file.
        """

        # launch winscp
        winscp_exe = exe_path

        # options and credentials
        cmds = ['option batch abort', 'option confirm off']
        connection = 'open ' + conn_str + '/ -hostkey="' + hostkey + '" -rawsettings ProxyMethod=2 ProxyHost="proxyfarm.xxxxx.com" ProxyPort=xxxxx'
        cmds.append(connection)

        if action.lower() == 'get':
            if match_all:
                cmds.append('cd ' + src_path)
                cmds.append('get ' + file_name + '*' + ' ' + dest_path + '\\')
            else:
                getpath = 'get ' + src_path + '\\' + file_name + ' ' + dest_path + '\\' + file_name
                cmds.append(getpath)

        elif action.lower() == 'put':
            if new_file_name:
                # create get path
                putpath = 'put ' + src_path + '\\' + file_name + ' ' + dest_path + '\\' + new_file_name
                cmds.append(putpath)
            else:
                putpath = 'put ' + src_path + '\\' + file_name + ' ' + dest_path + '\\' + file_name
                cmds.append(putpath)

        # create subprocess object
        winscp = Popen(winscp_exe, stdin=PIPE, stdout=PIPE, stderr=PIPE, universal_newlines=True)
        stdout, stderr = winscp.communicate('\n'.join(cmds))

        # check output
        if winscp.returncode:
            print("Upload Failed")
        else:
            out = stdout.splitlines()
            for i in out[15:]:
                print(i)


class Web:
    """
    Functions for interacting with files on the Web.

    """

    @classmethod
    def dl_google_sheet(cls, gid, gs, chromedriver):
        """
        Downloads a Google sheet as a .CSV file.

        :param gid: Id of Google Sheet to download.
        :param gs: Id of Google Sheet Tab to download.
        :param chromedriver: Chromedriver path to load Selenium from.
        :return: Downloaded Google Sheet tab.
        """

        browser = webdriver.Chrome(chromedriver)
        download_url = 'https://docs.google.com/spreadsheets/d/'
        download_url = download_url + gid
        download_url = download_url + '/gviz/tq?tqx=out:csv&sheet='
        download_url = download_url + gs

        browser.get(download_url)

    @classmethod
    def download_file(cls, sav_dir, url):
        """
        Downloads a file from a given Url endpoint.

        :param sav_dir: Filepath to save downloaded file to.
        :param url: URL to download file from.
        :return: Downloaded file in directory.
        """

        print(f"Downloading file at: {url}")
        response = requests.get(url)
        with open(f"{sav_dir}", 'wb') as f:
            f.write(response.content)
