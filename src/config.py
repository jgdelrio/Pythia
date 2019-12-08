import logging
from os import getenv, path
from datetime import datetime

from src.utils import get_logger


DEFAULT_HEADER = ("Content-type", 'text/plain; charset=utf-8')
DEFAULT_UTC_TS = datetime.utcfromtimestamp(datetime.min.toordinal())
ROOT = path.abspath(path.join(path.dirname(__file__), path.pardir))

ENV = getenv("ENV", "local")
MAX_CONNECTIONS = int(getenv("MAX_CONNECTIONS", "10"))
MIN_WAIT = int(getenv("MIN_WAIT", "3"))
VERBOSE = int(getenv("VERBOSE", "1"))

if ENV == "local":
    LOG_LEVEL = logging.DEBUG
elif ENV == "prod":
    LOG_LEVEL = logging.INFO


LOG = get_logger(name="Pythia", to_stdout=True, level=LOG_LEVEL)
