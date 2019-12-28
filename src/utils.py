import os
import sys
import re
import inspect
import logging
import pathlib
import traceback
import pandas as pd
from datetime import datetime, timedelta
from traitlets.config.loader import LazyConfigValue

from src.config import DFT_UTC_TS, LOG_LEVEL, LOG_FOLDER, VERBOSE


dateparse = lambda dates: pd.datetime.strptime(dates, '%Y-%m-%d')
capture_enum_regex = re.compile("^[\w]*\.\s*")


def get_logger(name="Pythia", to_stdout=False, level=LOG_LEVEL):
    """Creates a logger with the given name"""
    log_file = LOG_FOLDER.joinpath("name" + ".log")
    # TODO: Print to file as well and receive verbose level in the method
    logger = logging.getLogger(name)
    logger.setLevel(level)
    if to_stdout:
        ch = logging.StreamHandler(sys.stdout)
        ch.setLevel(level)
        logger.addHandler(ch)
    return logger


def bigint2utctimestamp(bigint):
    if bigint is None:
        return DFT_UTC_TS
    elif isinstance(bigint, str):
        bigint = int(bigint)
    return datetime.utcfromtimestamp(bigint / 1e3)


def in_ipynb(verbose=VERBOSE):
    """Detects if we are running within ipython (Notebook)"""
    try:
        cfg = get_ipython().config
        if isinstance(cfg['IPKernelApp']['parent_appname'], LazyConfigValue):
            if verbose > 2:
                print("Notebook detected")
            return True
        else:
            if verbose > 2:
                print("Running in script mode")
            return False
    except NameError:
        return False


def clean_enumeration(data):
    if isinstance(data, dict):
        return {key.replace(get_index(capture_enum_regex.findall(key), 0, ""), ""): val for key, val in data.items()}
    elif isinstance(data, list):
        return [c.replace(get_index(capture_enum_regex.findall(c), 0, ""), "") for c in data]
    else:
        raise Exception(f"Type of data not supported {type(data)}")


def clean_pandas_data(dat):
    """Receives a dictionary of data, transform the dict into a pandas DataFrame and clean the column names"""
    try:
        data = pd.DataFrame.from_dict(dat, orient="index")
        # Apply clean names to columns and index
        column_names = clean_enumeration(data.columns.tolist())
        data.columns = column_names
        data.index.name = 'date'
        data.sort_index(axis=0, inplace=True, ascending=True)  # Sort by date
    except Exception as err:
        LOG.error(f"Error cleaning dataset: {err}")
        data = None
    return data


def read_pandas_data(file_name):
    if not file_name.exists():
        LOG.error(f"ERROR: data not found for {file_name}")
        return None
    return pd.read_csv(file_name, parse_dates=['date'], index_col='date', date_parser=dateparse)


def save_pandas_data(file_name, dat, old_data=None, verbose=VERBOSE):
    try:
        data = clean_pandas_data(dat)

        if old_data is not None:
            try:
                # Avoid the last index as it may contain an incomplete week or month
                last_dt = old_data.index[-2]
                idx = data.index.get_loc(last_dt.strftime("%Y-%m-%d"))
                updated_data = pd.concat((old_data.iloc[:-2, :], data.iloc[idx:, :]), axis=0)
                updated_data.reset_index().to_csv(file_name, index=False, compression="infer")  # Update
            except KeyError as err:
                LOG.error(f"Error updating the data: {err}")
        else:
            data.reset_index().to_csv(file_name, index=False, compression="infer")          # Save

        if verbose > 1:
            symbol = file_name.parent.name
            LOG.info(f"Saved {symbol} data:{get_tabs(symbol, prev=12)}[{file_name.stem}] OK")
    except Exception as err:
        LOG.error(f"ERROR saving data:\t\t{file_name.parent.name + file_name.stem} "
                  f"{err.__repr__()} {traceback.print_tb(err.__traceback__)}")


class DelayedAssert:
    """
    Assert multiple conditions and report only after evaluating all of them

    Main 2 methods:
        expect(expr, msg=None)
        : Evaluate 'expr' as a boolean, and keeps track of failures

        assert_expectations()
        : raises an assert if an expect() calls failed

    Example:
        delayAssert = DelayedAssert()
        delayAssert.expect(3 == 1, 'Three differs from one')
        delayAssert.assert_expectations()
    """

    def __init__(self):
        self._failed_expectations = []

    def expect(self, expr, msg=None):
        """ keeps track of failed expectations """
        if not expr:
            self._log_failure(msg)

    def assert_expectations(self):
        """raise an assert if there are any failed expectations"""
        if self._failed_expectations:
            raise AssertionError(self._report_failures())

    def _log_failure(self, msg=None):
        (filename, line, funcname, contextlist) = inspect.stack()[2][1:5]
        filename = os.path.basename(filename)
        # context = contextlist[0].split('.')[0].strip()
        self._failed_expectations.append(
            'file "%s", line %s, in %s()%s'
            % (filename, line, funcname, (("\n\t%s" % msg) if msg else ""))
        )

    def _report_failures(self):
        if self._failed_expectations:
            (filename, line, funcname) = inspect.stack()[2][1:4]
            report = [
                "\n\nassert_expectations() called from",
                '"%s" line %s, in %s()\n'
                % (os.path.basename(filename), line, funcname),
                "Failed Expectations:%s\n" % len(self._failed_expectations),
            ]

            for i, failure in enumerate(self._failed_expectations, start=1):
                report.append("%d: %s" % (i, failure))
            self._failed_expectations = []
            return "\n".join(report)


def get_tabs(symbol, prev=7):
    n = len(symbol) + prev

    if in_ipynb():
        if n <= 15:
            return "\t" * 4
        elif n <= 21:
            return "\t" * 3
        elif n <= 27:
            return "\t" * 2
        elif n <= 33:
            return "\t"
        else:
            return ""
    else:
        if n <= 15:
            return "\t" * 4
        elif n <= 19:
            return "\t" * 3
        elif n <= 23:
            return "\t" * 2
        elif n <= 27:
            return "\t"
        else:
            return ""


def get_index(array, index, default=""):
    try:
        return array[index]
    except IndexError:
        return default


def datetime_format(date):
    if isinstance(date, datetime):
        return "%Y-%m-%d %H:%M:%S"
    else:
        return "%Y-%m-%d"


def ts2datetime(ts):
    return ts.strftime("%Y-%m-%dT%H:%M:%S")              #  '1999-12-13T00:00:00'


def datetime2ts(dt):
    return datetime.strptime(dt, "%Y-%m-%dT%H:%M:%S")


def first_day_of_month(day):
    return day.replace(day=1)


def last_day_of_month(day):
    next_month = day.replace(day=28) + timedelta(days=4)
    return next_month - timedelta(days=next_month.day)


def start_and_end_of_week(day):
    start_of_week = day - timedelta(days=day.weekday())
    end_of_week = start_of_week + timedelta(days=6)
    return start_of_week, end_of_week


def start_of_week(day):
    return start_and_end_of_week(day)[0]


def end_of_week(day):
    return start_and_end_of_week(day)[1]


def add_first_ts(info, first_date):
    """"""
    if not isinstance(info, dict):
        raise TypeError(f"info must be a dict, not {type(info)}")
    if not isinstance(first_date, datetime):
        LOG.error(f"first_date must be a datetime, not {type(first_date)}")
        return info

    if "FirstTimeStamp" in info.keys():
        prev_first_ts = info["FirstTimeStamp"]
        first_date = prev_first_ts if prev_first_ts <= first_date else first_date

    info["FirstTimeStamp"] = ts2datetime(first_date)
    return info


def cycle(array):
    """Cycle throughout the array indefinitely"""
    idx, n = 0, len(array)
    while True:
        if idx < n:
            yield array[idx]
            idx += 1
        else:
            yield array[0]
            idx = 1


LOG = get_logger(name="Pythia", to_stdout=True, level=LOG_LEVEL)
