"""
Logging tasks.

|pic1|
    .. |pic1| image:: ../images_source/logging.png
        :width: 30%

"""

import logging
import sys
import importlib
import pandas as pd
from datetime import datetime


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


def log_tbl_dict(proc_owner, proc_cat, proc_name, rundate=False, proc_runtime=False,
                 max_completed_step=False, outcome=False,
                 notes1_key=False, notes1_note=False, notes1_val=False,
                 notes2_key=False, notes2_note=False, notes2_val=False,
                 notes3_key=False, notes3_note=False, notes3_val=False,
                 notes4_key=False, notes4_note=False, notes4_val=False,
                 notes5_key=False, notes5_note=False, notes5_val=False,
                 notes6_key=False, notes6_note=False, notes6_val=False,
                 notes7_key=False, notes7_note=False, notes7_val=False,
                 notes8_key=False, notes8_note=False, notes8_val=False,
                 notes9_key=False, notes9_note=False, notes9_val=False,
                 notes10_key=False, notes10_note=False, notes10_val=False):
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

    dat = ({
        "proc_owner": proc_owner,
        "proc_cat": proc_cat,
        "proc_name": proc_name,
        "rundate": rundate,
        "proc_runtime": proc_runtime,
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

    df = pd.DataFrame(dat.items()).T
    df.columns = dat.keys()
    df = df.iloc[1:]
    df.replace(False, '', inplace=True)

    return df
