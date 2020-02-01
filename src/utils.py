import os
import sys
import re
import json
import inspect
import logging
import pathlib
import traceback
import numpy as np
import pandas as pd
from datetime import datetime, timedelta
from traitlets.config.loader import LazyConfigValue

from src.config import DFT_UTC_TS, LOG_LEVEL, LOG_FOLDER, LOGGER_FORMAT, VERBOSE


dateparse = lambda dates: pd.datetime.strptime(dates, '%Y-%m-%d')
capture_enum_regex = re.compile("^[\w]*\.\s*")


def validate_type(var, istype, name):
    if not isinstance(var, istype):
        var_name = istype.__name__ if not isinstance(istype, tuple) else istype[0].__name__
        raise TypeError(f"{name} symbol must be a {var_name}")


def get_logger(name="Pythia", to_stdout=False, level=LOG_LEVEL):
    """Creates a logger with the given name"""
    logging.basicConfig(format=LOGGER_FORMAT, datefmt="[%H:%M:%S]")
    # TODO: Print to file as well and receive verbose level in the method
    logger = logging.getLogger(name)
    logger.setLevel(level)

    # File log
    log_filename = "pythia_" + datetime.now().strftime("%Y-%m-%d_%H-%M")
    file_handler = logging.FileHandler(f"{LOG_FOLDER}/{log_filename}.log")
    logfile_formatter = logging.Formatter("%(asctime)s [%(threadName)-12.12s] [%(levelname)-5.5s]  %(message)s")
    file_handler.setFormatter(logfile_formatter)
    logger.addHandler(file_handler)
    # logger.addHandler(logging.StreamHandler(log_file))

    if to_stdout or in_ipynb():
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


def dict2pandas_data(dat):
    """Receives a dictionary of data, transform the dict into a pandas DataFrame and clean the column names"""
    try:
        data = pd.DataFrame.from_dict(dat, orient="index")
        # Apply clean names to columns and index
        column_names = clean_enumeration(data.columns.tolist())
        data.columns = column_names
        data.index.name = "date"
        data.index = list(map(dateparse, data.index))
        data.sort_index(axis=0, inplace=True, ascending=True)  # Sort by date
        data = transform_column_types(data)
    except Exception as err:
        LOG.error(f"Error cleaning dataset: {err}")
        data = None
    return data


def read_pandas_data(file_name):
    if not file_name.exists():
        LOG.error(f"ERROR: data not found for {file_name}")
        return None
    try:
        return pd.read_csv(file_name, parse_dates=['date'], index_col='date', date_parser=dateparse)

    except ValueError as err:
        if err.args[0] == "'date' is not in list":
            data = pd.read_csv(file_name, parse_dates=['index'], index_col='index', date_parser=dateparse)
            data.index.name = "date"
            return data
        else:
            LOG.error(f"Error reading the file {file_name}: {err}")
            raise Exception(err)

    except Exception as err:
        LOG.error(f"Error reading the file {file_name}: {err}")
        raise Exception(err)


def save_pandas_data(file_name, data, old_data=None, verbose=VERBOSE):
    # Ensure indices names are aligned
    data.index.name = "date"
    try:
        if old_data is not None:
            try:
                old_data.index.name = "date"
                # Avoid the last index as it may contain an incomplete week or month
                last_dt = old_data.index[-2]
                idx = data.index.get_loc(last_dt.strftime("%Y-%m-%d"))
                updated_data = pd.concat((old_data.iloc[:-2, :], data.iloc[idx:, :]), axis=0)
                updated_data.sort_index(axis=0, inplace=True)
                updated_data.reset_index().to_csv(file_name, index=False, compression="infer")
            except KeyError as err:
                LOG.error(f"Error updating the data: {err}")
        else:
            data.reset_index().to_csv(file_name, index=False, compression="infer")

        if verbose > 1:
            symbol = file_name.parent.name
            LOG.info(f"Saved {symbol} data:{get_tabs(symbol, prev=12)}[{file_name.stem}] OK")
    except Exception as err:
        LOG.error(f"ERROR saving data:\t\t{file_name.parent.name + file_name.stem} "
                  f"{err.__repr__()} {traceback.print_tb(err.__traceback__)}")


def compare_dfs_by_index(df_base, df_new, symbol, maxerror=1e-3, raiseerror=False):
    """
    Compare index by index and column by column the differences of two dataframes
    :param df_base:    base dataframe
    :param df_new:     new dataframe
    :param maxerror:   maximum admissible error
    :param raiseerror: if true, raise an error when maxerror is reached, otherwise return 1 for equality and -1 for inequality
    """
    # Verify columns:
    ind = [k not in df_new.columns for k in df_base.columns]
    if any(ind):
        raise ValueError(f"{symbol} new dataframe don't have all columns available in base dataframe. "
                         f"Missing: {df_base.columns[ind]}")

    accum_errors = 0
    error_ref = []

    # Find similar indexes
    ind = [k for k in df_new.index if k in df_base.index]
    cmp_df_base = df_base[df_base.index.isin(ind)]
    cmp_df_new = df_new[df_new.index.isin(ind)]
    for cn in cmp_df_new.columns:
        error_ind = (abs(cmp_df_base[cn] - cmp_df_new[cn])) >= maxerror
        n_error = sum(error_ind)
        if n_error > 0:
            ts_positions = [ke for eind, ke in zip(error_ind, ind) if eind]
            LOG.warning(f"{symbol} prev. data error:{get_tabs(symbol, prev=18)}At '{cn}' in positions {ts_positions}")
            accum_errors += n_error
            error_ref.extend([(cn, pos) for pos in ts_positions])

    if accum_errors > 0 and raiseerror:
        raise Exception(f"Data in new dataframe differ from base data. Number of errors: {accum_errors}")
    else:
        return accum_errors, error_ref


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


def get_tabs(param, prev=7):
    if isinstance(param, (int, float)):
        n = len(str(param)) + prev
    else:
        n = len(param) + prev

    if in_ipynb():
        if n <= 15:
            return "\t" * 5
        elif n <= 19:
            return "\t" * 4
        elif n <= 23:
            return "\t" * 3
        elif n <= 27:
            return "\t" * 2
        elif n <= 33:
            return "\t"
        else:
            return ""
    else:
        if n <= 4:
            return "\t" * 7
        elif n <= 8:
            return "\t" * 6
        elif n <= 12:
            return "\t" * 5
        elif n <= 16:
            return "\t" * 4
        elif n <= 20:
            return "\t" * 3
        elif n <= 24:
            return "\t" * 2
        elif n <= 28:
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
        prev_first_ts = datetime2ts(info["FirstTimeStamp"])
        first_date = prev_first_ts if prev_first_ts <= first_date else first_date

    info["FirstTimeStamp"] = ts2datetime(first_date)
    return info


def map_field(array, field):
    return [x.get(field, None) for x in array]


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


def transform_column_types(data):
    column_names = data.columns

    for cn in column_names:
        try:
            data[cn] = data[cn].astype(int)
        except ValueError:
            try:
                data[cn] = data[cn].astype(float)
            except Exception:
                pass
    return data


def transform2yaml(val):
    if val == "":
        return val
    elif isinstance(val, str):
        if re.match(r"\d", val[0]):
            return f"'{val}'"
        else:
            return val
    else:
        return val


def custom_yaml_text(array, order):
    output = ""
    for stock in array:
        first = False
        for key in order:
            if first is False:
                output += f"- {key}: {transform2yaml(stock[key])}\n"
                first = True
            else:
                output += f"  {key}: {transform2yaml(stock[key])}\n"
    return output


class NpEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, np.integer):
            return int(obj)
        elif isinstance(obj, np.floating):
            return float(obj)
        elif isinstance(obj, np.ndarray):
            return obj.tolist()
        else:
            return super(NpEncoder, self).default(obj)


def check_monotonic_ts_index(df, symbol, category):
    if not df.index.is_monotonic:
        LOG.error(f"Non monotonic index detected for {symbol} {category}")
        raise ValueError(f"Non monotonic indices at {symbol}")


LOG = get_logger(name="Pythia", to_stdout=False, level=LOG_LEVEL)
