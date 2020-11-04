"""
Tools for interacting with a computer' screen elements.

|pic1|
    .. |pic1| image:: ../images_source/screen_tools/screen.png
        :width: 25%
"""

import os
import time
from pathlib import Path
from subprocess import Popen, PIPE
import numpy as np
import pandas as pd
import pyautogui as gui


class Open:
    """
    Functions for opening files and applications on the OS.

    """

    @classmethod
    def text_to_vscode(cls, txt, name=False, extension=False):
        """
        Exports a string to Visual Studio Code.

        :param txt: String to export.
        :param name: Name for text file (optional).
        :param extension: Extension for text file (optional).
        :return: Opens a Visual Studio Code instance.
        """

        path_open = os.getcwd()

        # path_parts = os.getcwd().split("\\")
        # path = ""
        # for idx, elem in enumerate(path_parts):
        #     if " " in elem:
        #         elem_new = f'''"{elem}"'''
        #     else:
        #         elem_new = elem
        #
        #     path = path + "\\" + elem_new
        #
        # path = path[path.find("\\") + 1:]

        txt = txt.replace("\n", " ")
        text_out = open(f'''{path_open}\\{name if name else "txt"}.{extension if extension else "txt"}''', 'w')

        text_out.write(txt)
        text_out.close()

        os.system(f'''code sql.sql''')


class Move:
    """
    FUnctions for moving files.

    """

    @classmethod
    def copy_paste(cls, dir, str_search, sav_dir=False, sav_name=False):
        """
        Copys data within an application window and pastes it in another.

        :param dir: Directory for files to copy.
        :param str_search: String to filter files.
        :param sav_dir: Directory to save file list to.
        :param sav_name: Filename to save file list to.
        :return: Progress for file copy and paste loop.
        """
        files = []

        # get all file names
        for filename in Path(dir).glob(f'**/*{str_search}'):
            files.append(filename)
            print(filename)

        # open them in code
        for idx, f in enumerate(files):
            time.sleep(3)
            os.system(f"code {f}")

            # select everything
            time.sleep(5)
            gui.hotkey('ctrl', 'a')
            time.sleep(0.5)

            # copy
            time.sleep(5)
            gui.hotkey('ctrl', 'c')

            # switch windows
            time.sleep(5)
            gui.hotkey("alt", "tab")

            # goto bottom
            time.sleep(5)
            gui.hotkey("ctrl", "end")
            gui.hotkey("ctrl", "enter")

            # write name
            time.sleep(5)
            gui.typewrite(f"#{f}")
            time.sleep(2)
            gui.hotkey("ctrl", "enter")

            # paste
            time.sleep(5)
            gui.hotkey("ctrl", "v")

            print(idx / len(files))

        if sav_dir:
            files_exp = pd.DataFrame({"file": files})
            files_exp.to_csv(f'{sav_dir}{sav_name}.csv', index=False)
