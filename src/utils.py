import os
import sys
import inspect
import logging
import pathlib
from datetime import datetime
from traitlets.config.loader import LazyConfigValue

from src.config import DFT_UTC_TS, LOG_LEVEL, VERBOSE


def get_logger(name="haar_training", to_stdout=False, level=LOG_LEVEL):
    """Creates a logger with the given name"""
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
            if verbose > 0:
                print("Notebook detected")
            return True
        else:
            if verbose > 0:
                print("Running in script mode")
            return False
    except NameError:
        return False


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
    if n <= 10:
        return "\t" * 4
    elif n <= 15:
        return "\t" * 3
    elif n <= 19:
        return "\t" * 2
    elif n <= 23:
        return "\t"
    else:
        return ""


LOG = get_logger(name="Pythia", to_stdout=True, level=LOG_LEVEL)
