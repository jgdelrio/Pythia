import sys
import pathlib
import logging
from datetime import datetime
from traitlets.config.loader import LazyConfigValue

from src.config import DEFAULT_UTC_TS, LOG_LEVEL, VERBOSE


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
        return DEFAULT_UTC_TS
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


LOG = get_logger(name="Pythia", to_stdout=True, level=LOG_LEVEL)
