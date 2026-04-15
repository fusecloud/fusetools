"""Transfer applications for local files, SSH, and SFTP."""

from __future__ import annotations

import os
import sys
from pathlib import Path
from subprocess import PIPE, Popen
from typing import Any, Optional

import pandas as pd


# MARK: - Access
class Access:
    """Functions for accessing file systems and protocols."""

    @classmethod
    def proxies(cls, domain: str) -> dict[str, str]:
        """Create an http/https proxy address.

        :param domain: domain address.
        :return: Http/https proxy address.
        """
        res = {
            "http": "http://" + os.environ["usr"] + ":" + os.environ["pwd"] + f"@proxyfarm.{domain}.com:8080",
            "https": "https://" + os.environ["usr"] + ":" + os.environ["pwd"] + f"@proxyfarm.{domain}.com:8080",
        }

        return res


# MARK: - Local
class Local:
    """Functions for accessing local files."""

    @classmethod
    def zip_dir(cls, directory_list: list[str], zipname: str) -> None:
        """Compress a directory into a single ZIP file.

        :param directory_list: List of files to compress into zip file.
        :param zipname: Name of zip file to compress files into.
        :return: Zip file containing files.
        """
        import zipfile

        out_zip = zipfile.ZipFile(zipname, "w", zipfile.ZIP_DEFLATED)

        for dir_ in directory_list:
            if not os.path.exists(dir_):
                print(f"Error, directory {dir_} does not exist")
                continue

            rootdir = os.path.basename(dir_)

            try:
                os.listdir(dir_)
                for dirpath, dirnames, filenames in os.walk(dir_):
                    for filename in filenames:
                        filepath = os.path.join(dirpath, filename)
                        parentpath = os.path.relpath(filepath, dir_)
                        arcname = os.path.join(rootdir, parentpath)
                        out_zip.write(filepath, arcname)
            except NotADirectoryError:
                out_zip.write(dir_, dir_.split("/")[-1])

        out_zip.close()

    @classmethod
    def clear_delete_directory(cls, directory: str, method: str = "delete") -> None:
        """Clear and/or delete a directory and its contents.

        :param directory: Filepath of directory.
        :param method: Optional delete for the directory folder.
        """
        directory_path = Path(directory)
        for item in directory_path.iterdir():
            if item.is_dir():
                Local.clear_delete_directory(str(item))
            else:
                item.unlink()

        if method == "delete":
            directory_path.rmdir()

    @classmethod
    def fix_user_path(cls, dir_: str) -> str:
        """Fix a local filepath.

        :param dir_: Directory to patch.
        :return: Patched directory.
        """
        dir_components = dir_.split("/")
        search = "Users"
        idx = 0
        for idx, elem in enumerate(dir_components):
            if elem == search:
                break

        dir_components[idx + 1] = os.environ["os_name"]
        r = "/".join(dir_components)

        return r

    @classmethod
    def get_all_filetimes(cls, dir_: str, exclude: Optional[str] = None) -> pd.DataFrame:
        """Create a DataFrame of filenames and file times in a given directory.

        :param dir_: Directory of files.
        :param exclude: A string to search for files to exclude.
        :return: DataFrame of filenames and file times.
        """
        files = os.listdir(dir_)

        if exclude:
            files = [f for f in files if exclude not in f]

        times = [os.path.getmtime(dir_ + f) for f in files]
        file_times = pd.DataFrame({"files": files, "times": times})
        return file_times

    @classmethod
    def get_latest_file(cls, name: str, dir_: str, exclude: Optional[str] = None) -> str:
        """Get the latest file in a directory.

        :param name: String match name of file(s).
        :param dir_: Directory for the file search.
        :param exclude: A string to search for files to exclude.
        :return: Name of most recent file.
        """
        name = name.lower()
        files = os.listdir(dir_)

        if exclude:
            files = [f for f in files if exclude not in f]

        times = [os.path.getmtime(dir_ + f) for f in files]
        file_times = pd.DataFrame({"files": files, "times": times})
        file_times["files_lower"] = file_times["files"].str.lower()
        file_times = file_times[file_times["files_lower"].str.contains(name)]
        read_file = file_times[file_times["times"] == max(file_times["times"])]["files"]
        read_file = read_file.values[0]

        return read_file

    @classmethod
    def read_files_like(
        cls,
        name: str,
        dir_: str,
        ext_typ: str,
        sheet_name: Any = False,
    ) -> pd.DataFrame:
        """Read and concatenate files in a directory that match a string.

        :param name: A string search to match.
        :param dir_: Directory to search for files in.
        :param ext_typ: Extension type to search for (.XLSX OR .CSV).
        :param sheet_name: Sheet number or name to read (for non-CSV).
        :return: Concatenated DataFrames that match a string.
        """
        files = os.listdir(dir_)
        files_df = pd.DataFrame({"files": files})
        files_df["files"] = files_df["files"].str.lower()
        files_df = files_df[(files_df["files"].str.contains(name)) & (files_df["files"].str.contains(ext_typ))]
        files_df.reset_index(inplace=True)

        dat_all = pd.DataFrame()
        for idx, f in files_df.iterrows():
            if ext_typ == "csv":
                dat = pd.read_csv(dir_ + f["files"])
            else:
                dat = pd.read_excel(dir_ + f["files"], sheet_name=sheet_name)
            if idx == 0:
                dat_all = dat
            else:
                dat_all = pd.concat([dat_all, dat])

        return dat_all


# MARK: - SSH
class SSH:
    """Functions for transferring data with SSH."""

    @classmethod
    def ssh_transfer_files(
        cls,
        local_file_path: str,
        target_file_path: str,
        target_user: str,
        target_ip: str,
        target_pwd: str,
        direction: str = "send",
    ) -> None:
        """Transfer a file via SSH.

        :param local_file_path: Filepath for the object to transfer on local machine.
        :param target_file_path: Filepath for the object on target machine.
        :param target_user: Username of target machine.
        :param target_ip: IP of target machine.
        :param target_pwd: Password of target machine.
        :param direction: Whether to send or receive (send or other).
        """
        import pexpect

        if direction == "send":
            cmd = f"scp -rp {local_file_path} {target_user}@{target_ip}:{target_file_path}"
        else:
            cmd = f"scp -r {target_user}@{target_ip}:{target_file_path} {local_file_path}"

        child = pexpect.spawn(cmd, encoding="utf-8")
        child.logfile = sys.stdout
        child.expect(".*password:", timeout=None)
        child.sendline(target_pwd)
        child.expect(pexpect.EOF, timeout=None)


# MARK: - SFTP
class SFTP:
    """Functions for using SFTP/FTP protocols."""

    @classmethod
    def winscp_sftp_connect(
        cls,
        action: str,
        exe_path: str,
        conn_str: str,
        hostkey: str,
        src_path: str,
        dest_path: str,
        file_name: str,
        new_file_name: Optional[str] = None,
        match_all: bool = False,
    ) -> None:
        """Connect to an SFTP server.

        :param action: Actions allowed are "put" or "get".
        :param exe_path: Path where WinSCP executable is installed.
        :param conn_str: Connection string to connect to ftp site.
        :param hostkey: HostKey credential required to match.
        :param src_path: Source path for file transfer.
        :param dest_path: Destination path for file transfer.
        :param file_name: File name with format extension.
        :param new_file_name: New file name (optional).
        :param match_all: Whether to match on wildcard.
        """
        winscp_exe = exe_path

        cmds = ["option batch abort", "option confirm off"]
        connection = "open " + conn_str + '/ -hostkey="' + hostkey + '" -rawsettings ProxyMethod=2 ProxyHost="proxyfarm.xxxxx.com" ProxyPort=xxxxx'
        cmds.append(connection)

        if action.lower() == "get":
            if match_all:
                cmds.append("cd " + src_path)
                cmds.append("get " + file_name + "*" + " " + dest_path + "\\")
            else:
                getpath = "get " + src_path + "\\" + file_name + " " + dest_path + "\\" + file_name
                cmds.append(getpath)

        elif action.lower() == "put":
            if new_file_name:
                putpath = "put " + src_path + "\\" + file_name + " " + dest_path + "\\" + new_file_name
                cmds.append(putpath)
            else:
                putpath = "put " + src_path + "\\" + file_name + " " + dest_path + "\\" + file_name
                cmds.append(putpath)

        winscp = Popen(
            winscp_exe,
            stdin=PIPE,
            stdout=PIPE,
            stderr=PIPE,
            universal_newlines=True,
        )
        stdout, stderr = winscp.communicate("\n".join(cmds))

        if winscp.returncode:
            print("Upload Failed")
        else:
            out = stdout.splitlines()
            for i in out[15:]:
                print(i)
