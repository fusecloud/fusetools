"""
Functions for interacting with Python date objects.

"""

import math
from datetime import datetime, timedelta


def get_last_dow(dow, ref_date=False):
    """
    Get the last date for a given day of the week (ex: Sunday, Monday)

    :param dow: Day of week to pull date for.
    :param ref_date: Date to provide a reference for (optional).
    :return: Last date for a given day of the week.
    """
    dd = {
        'monday': 7,
        'tuesday': 1,
        'wednesday': 2,
        'thursday': 3,
        'friday': 4,
        'saturday': 5,
        'sunday': 6
    }

    if not ref_date:
        d = datetime.today()
    else:
        d = datetime.strptime(ref_date, "%Y-%m-%d")

    # backtrack to last sunday
    offset = (d.weekday() - dd.get("sunday")) % 7
    last_sunday = d - timedelta(days=offset)

    offset = (last_sunday.weekday() - dd.get(dow)) % 7
    date = str(last_sunday - timedelta(days=offset))[:10]

    return date


def get_rptg_qtr(ref_date=False):
    """
    Get the reporting year/quarter combination for a given date.

    :param ref_date: Date to provide a reference for (optional).
    :return: Reporting year/quarter combination.
    """
    if ref_date:
        date = datetime.strptime(ref_date, '%Y-%m-%d')
        year = str(date.year)
        qtr = str(math.ceil(date.month / 3)).zfill(2)
    else:
        year = str(datetime.now().year)
        qtr = str(math.ceil(datetime.now().month / 3)).zfill(2)

    if qtr == "01":
        qtr = "04"
        year = str(int(year) - 1)

    date = year + qtr

    return date


def get_rptg_mon(ref_date=False):
    """
     Get the reporting year/month combination for a given date.

    :param ref_date: Date to provide a reference for (optional).
    :return: Reporting year/month combination.
    """
    if ref_date:
        date = datetime.strptime(ref_date, '%Y-%m-%d')
    else:
        date = int(datetime.now().strftime('%Y%m'))
        if int(str(date)[-2:]) == 1:
            date = str(int(str(date)[:4]) - 1) + \
                   "12"
        else:
            date = str(int(str(date)[:4])) + str(int(str(date)[-2:]) - 1).zfill(2)

    return date


def get_rptg_yr(ref_date=False):
    """
    Get the reporting year for a given date.

    :param ref_date: Date to provide a reference for (optional).
    :return: Reporting year for a given date.
    """
    if ref_date:
        date = int(datetime.strptime(ref_date, '%Y'))
    else:
        date = int(datetime.now().strftime('%Y'))

    if get_rptg_week()[4:6] == 52:
        date = date - 1

    return date


def get_rptg_week(ref_date=False):
    """
    Get the reporting year/week combination for a given date.

    :param ref_date: Date to provide a reference for (optional).
    :return: Reporting year/week combination for a given date.
    """
    if ref_date:
        date = datetime.strptime(ref_date, '%Y-%m-%d')
        # if ref date, find next sunday
        rel_sunday = date + timedelta((6 - date.weekday()) % 7)
        # rel_sunday.strftime("%Y-%m-%d")
    else:
        date = datetime.now()
        # if today, find last sunday
        offset = (date.weekday() - 6) % 7
        rel_sunday = date - timedelta(days=offset)

    rpt_year = rel_sunday.strftime("%Y")
    rpt_month = rel_sunday.strftime("%m")
    rpt_week = rel_sunday.strftime("%V")

    if rpt_month == '12' and rpt_week == '01':
        rpt_year = str(int(rpt_year) + 1)

    return rpt_year + rpt_week
