"""
Logging tasks.

|pic1|
    .. |pic1| image:: ../images_source/logging.png
        :width: 30%

"""

import logging
import sys
import importlib
from typing import List, Optional

import pandas as pd
from datetime import datetime
from logging import Filter
import threading


class ThreadFilter(Filter):
    """Only accept log records from a specific thread or thread name"""

    def __init__(self, threadid=None, threadname=None):
        if threadid is None and threadname is None:
            raise ValueError("Must set at a threadid and/or threadname to filter on")
        self._threadid = threadid
        self._threadname = threadname

    def filter(self, record):
        if self._threadid is not None and record.thread != self._threadid:
            return False
        if self._threadname is not None and record.threadName != self._threadname:
            return False
        return True


class IgnoreThreadsFilter(Filter):
    """Only accepts log records that originated from the main thread"""

    def __init__(self):
        self._main_thread_id = threading.main_thread().ident

    def filter(self, record):
        return record.thread == self._main_thread_id


def make_script_function(module, path):
    """
    Execute a Python file as a function.

    :param module: Arbitrary name for file to run.
    :param path: Filepath for file to run.
    :return: Executed run of a Python file.
    """

    spec = importlib.util.spec_from_file_location(module, path)
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module

    def function():
        spec.loader.exec_module(module)

    return function


def log_setup(name, filename, warning_type):
    """
    Creates a logger object with specified parameters.

    :param name: Name of logger object.
    :param filename: Filename of logging contents.
    :param warning_type: Level of warnings to provide.
    :return: Logger object with specified parameters.
    """

    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')

    file_handler = logging.FileHandler(filename)
    file_handler.setFormatter(formatter)

    logger_obj = logging.getLogger(name)

    if warning_type == "INFO":
        logger_obj.setLevel(logging.INFO)
    elif warning_type == "WARNING":
        logger_obj.setLevel(logging.WARNING)
    elif warning_type == "CRITICAL":
        logger_obj.setLevel(logging.CRITICAL)
    elif warning_type == "DEBUG":
        logger_obj.setLevel(logging.DEBUG)
    elif warning_type == "ERROR":
        logger_obj.setLevel(logging.ERROR)

    logger_obj.addHandler(file_handler)

    return logger_obj


def log_all_thread(filename,
                   level='INFO',
                   thread_type="main",
                   main_log=False):
    """
    Logs all specified data on a 'main' thread or a 'sub' thread. Main thread excluded logging from other threads. Sub thread logs data for only that threadId.

    :param filename: Filename to log the thread contents to.
    :param level: Level of logging for the thread (ie: INFO, DEBUG, WARNING, etc)
    :param thread_type: 'Main' thread or 'Sub' thread.
    :param main_log: Main log to specify if using a sub thread.
    :return: A logfile with thread's output.
    """

    if thread_type == "main":
        # ignores other threads
        thread_handler = logging.FileHandler(filename, 'a')
        thread_handler.addFilter(IgnoreThreadsFilter())

        logging.basicConfig(
            handlers=[thread_handler],
            level=logging.INFO)
        logger = logging.getLogger()

        if level == "INFO":
            logger.setLevel(logging.INFO)
        elif level == "WARNING":
            logger.setLevel(logging.WARNING)
        elif level == "CRITICAL":
            logger.setLevel(logging.CRITICAL)
        elif level == "DEBUG":
            logger.setLevel(logging.DEBUG)
        elif level == "ERROR":
            logger.setLevel(logging.ERROR)

        return logger

    else:
        # listens to only a sub thread thread
        thread_handler = logging.FileHandler(filename, 'a')
        thread_handler.addFilter(ThreadFilter(threadid=threading.get_ident()))
        main_log.addHandler(thread_handler)

        return main_log


def log_all(filename=False):
    """
    Log all stdout to a local logfile.

    :param filename: Filename to write stdout content to.
    :return: Logging contents to a local file.
    """

    dt_str = str(datetime.now()).replace("-", "").replace(":", "").replace(".", "").replace(" ", "_")
    dt_str = dt_str + ".log"

    if filename:
        logger = logging.getLogger()

        sys.stderr.write = logger.error
        sys.stdout.write = logger.info

        logging.basicConfig(filename=filename + "_" + dt_str, level=logging.INFO)


def log_tbl_df(
        proc_owner: Optional[list] = None,
        proc_cat: Optional[list] = None,
        proc_name: Optional[list] = None,
        run_datetime: Optional[list] = None,
        elapsed_run_time: Optional[list] = None,
        max_completed_step: Optional[list] = None,
        outcome: Optional[list] = None,
        notes1_key: Optional[list] = None,
        notes1_note: Optional[list] = None,
        notes1_val: Optional[list] = None,
        notes2_key: Optional[list] = None,
        notes2_note: Optional[list] = None,
        notes2_val: Optional[list] = None,
        notes3_key: Optional[list] = None,
        notes3_note: Optional[list] = None,
        notes3_val: Optional[list] = None,
        notes4_key: Optional[list] = None,
        notes4_note: Optional[list] = None,
        notes4_val: Optional[list] = None,
        notes5_key: Optional[list] = None,
        notes5_note: Optional[list] = None,
        notes5_val: Optional[list] = None,
        notes6_key: Optional[list] = None,
        notes6_note: Optional[list] = None,
        notes6_val: Optional[list] = None,
        notes7_key: Optional[list] = None,
        notes7_note: Optional[list] = None,
        notes7_val: Optional[list] = None,
        notes8_key: Optional[list] = None,
        notes8_note: Optional[list] = None,
        notes8_val: Optional[list] = None,
        notes9_key: Optional[list] = None,
        notes9_note: Optional[list] = None,
        notes9_val: Optional[list] = None,
        notes10_key: Optional[list] = None,
        notes10_note: Optional[list] = None,
        notes10_val: Optional[list] = None, ):
    """
    Creates a Pandas DataFrame of logging details.

    :param proc_owner:
    :param proc_cat:
    :param proc_name:
    :param rundate:
    :param proc_runtime:
    :param max_completed_step:
    :param outcome:
    :param notes1_key:
    :param notes1_note:
    :param notes1_val:
    :param notes2_key:
    :param notes2_note:
    :param notes2_val:
    :param notes3_key:
    :param notes3_note:
    :param notes3_val:
    :param notes4_key:
    :param notes4_note:
    :param notes4_val:
    :param notes5_key:
    :param notes5_note:
    :param notes5_val:
    :param notes6_key:
    :param notes6_note:
    :param notes6_val:
    :param notes7_key:
    :param notes7_note:
    :param notes7_val:
    :param notes8_key:
    :param notes8_note:
    :param notes8_val:
    :param notes9_key:
    :param notes9_note:
    :param notes9_val:
    :param notes10_key:
    :param notes10_note:
    :param notes10_val:
    :return:
    """

    # ddl = '''
    # create table logging_details (
    # proc_owner VARCHAR(100),
    # proc_cat VARCHAR (100),
    # proc_name VARCHAR (100),
    # rundate VARCHAR (100),
    # proc_runtime VARCHAR (100),
    # max_completed_step VARCHAR (100),
    # outcome VARCHAR (100),
    # notes1_key VARCHAR (100),
    # notes1_note VARCHAR (100),
    # notes1_val VARCHAR (100),
    # notes2_key VARCHAR (100),
    # notes2_note VARCHAR (100),
    # notes2_val VARCHAR (100),
    # notes3_key VARCHAR (100),
    # notes3_note VARCHAR (100),
    # notes3_val VARCHAR (100),
    # notes4_key VARCHAR (100),
    # notes4_note VARCHAR (100),
    # notes4_val VARCHAR (100),
    # notes5_key VARCHAR (100),
    # notes5_note VARCHAR (100),
    # notes5_val VARCHAR (100),
    # notes6_key VARCHAR (100),
    # notes6_note VARCHAR (100),
    #  notes6_val VARCHAR (100),
    #  notes7_key VARCHAR (100),
    #  notes7_note VARCHAR (100),
    #  notes7_val VARCHAR (100),
    #  notes8_key VARCHAR (100),
    #  notes8_note VARCHAR (100),
    #  notes8_val VARCHAR (100),
    #  notes9_key VARCHAR (100),
    #  notes9_note VARCHAR (100),
    #  notes9_val VARCHAR (100),
    #  notes10_key VARCHAR (100),
    #  notes10_note VARCHAR (100),
    #  notes10_val VARCHAR (100)
    # )
    # '''

    df = pd.DataFrame({
        "proc_owner": proc_owner,
        "proc_cat": proc_cat,
        "proc_name": proc_name,
        "run_datetime": run_datetime,
        "elapsed_run_time": elapsed_run_time,
        "max_completed_step": max_completed_step,
        "outcome": outcome,
        "notes1_key": notes1_key,
        "notes1_note": notes1_note,
        "notes1_val": notes1_val,
        "notes2_key": notes2_key,
        "notes2_note": notes2_note,
        "notes2_val": notes2_val,
        "notes3_key": notes3_key,
        "notes3_note": notes3_note,
        "notes3_val": notes3_val,
        "notes4_key": notes4_key,
        "notes4_note": notes4_note,
        "notes4_val": notes4_val,
        "notes5_key": notes5_key,
        "notes5_note": notes5_note,
        "notes5_val": notes5_val,
        "notes6_key": notes6_key,
        "notes6_note": notes6_note,
        "notes6_val": notes6_val,
        "notes7_key": notes7_key,
        "notes7_note": notes7_note,
        "notes7_val": notes7_val,
        "notes8_key": notes8_key,
        "notes8_note": notes8_note,
        "notes8_val": notes8_val,
        "notes9_key": notes9_key,
        "notes9_note": notes9_note,
        "notes9_val": notes9_val,
        "notes10_key": notes10_key,
        "notes10_note": notes10_note,
        "notes10_val": notes10_val
    })

    return df
