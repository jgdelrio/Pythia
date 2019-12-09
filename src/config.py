import logging
from os import getenv
import pathlib
from datetime import datetime


DEFAULT_INFO_FILE = "info_data.json"
DEFAULT_STOCK_FILE = "stock_data.zip"
DEFAULT_HEADER = ("Content-type", 'text/plain; charset=utf-8')
DEFAULT_UTC_TS = datetime.utcfromtimestamp(datetime.min.toordinal())
ROOT = pathlib.Path(__file__).parents[1]
DATA_FOLDER = ROOT.joinpath("data")

ENV = getenv("ENV", "local")
MAX_CONNECTIONS = int(getenv("MAX_CONNECTIONS", "10"))
MIN_SEM_WAIT = int(getenv("MIN_WAIT", "2"))
VERBOSE = int(getenv("VERBOSE", "3"))

if ENV == "local":
    LOG_LEVEL = logging.DEBUG
elif ENV == "prod":
    LOG_LEVEL = logging.INFO

HEADERS = {
    'user-agent': ('Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_5) '
                   'AppleWebKit/537.36 (KHTML, like Gecko) '
                   'Chrome/45.0.2454.101 Safari/537.36'),
}
