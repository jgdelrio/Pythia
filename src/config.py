import yaml
import logging
from os import getenv
import pathlib
from datetime import datetime


# DEFAULT PARAMETERS
DFT_INFO_FILE = "info_data"
DFT_INFO_EXT = ".json"
DFT_STOCK_FILE = "stock_data"
DFT_STOCK_EXT = ".zip"
DFT_FX_FILE = "fx_data"
DFT_FX_EXT = ".zip"
DFT_HEADER = ("Content-type", 'text/plain; charset=utf-8')
DFT_UTC_TS = datetime.utcfromtimestamp(datetime.min.toordinal())

ROOT = pathlib.Path(__file__).parents[1]
DATA_FOLDER = ROOT.joinpath("data")

ENV = getenv("ENV", "local")
MAX_CONNECTIONS = int(getenv("MAX_CONNECTIONS", "10"))
MIN_SEM_WAIT = int(getenv("MIN_WAIT", "2"))
VERBOSE = int(getenv("VERBOSE", "2"))

if ENV == "local":
    LOG_LEVEL = logging.DEBUG
elif ENV == "prod":
    LOG_LEVEL = logging.INFO

HEADERS = {
    'user-agent': ('Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_5) '
                   'AppleWebKit/537.36 (KHTML, like Gecko) '
                   'Chrome/45.0.2454.101 Safari/537.36'),
}

KEYS_FILE = ROOT.joinpath("keys.yml")

if KEYS_FILE.exists():
    with open(KEYS_FILE, mode="r") as f:
        KEYS_SET = yaml.load(f, Loader=yaml.FullLoader)
else:
    raise Exception("ERROR: Please create the file 'keys.yml' at the root of the repository with valid keys")
