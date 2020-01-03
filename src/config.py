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
DFT_FX_FILE = "data"
DFT_FX_EXT = ".zip"
DFT_CRIPTO_PREFIX = "CRYPTO_"
INFO_VARIATIONS = ["daily", "daily-adjusted", "weekly", "weekly-adjusted", "monthly", "monthly-adjusted"]
FX_VARIATIONS = ["daily", "weekly", "monthly"]
FX_FEE = 0.012
DFT_HEADER = ("Content-type", 'text/plain; charset=utf-8')
DFT_UTC_TS = datetime.utcfromtimestamp(datetime.min.toordinal())

ROOT = pathlib.Path(__file__).parents[1]
DATA_FOLDER = ROOT.joinpath("data")
LOG_FOLDER = ROOT.joinpath("logs")
PORTFOLIOS_FOLDER = ROOT.joinpath("portfolios")
REPORTS_FOLDER = ROOT.joinpath("reports")
ISIN_DB = "DB_ISIN.csv"
MISSING_DB = "DB_missing.csv"

ENV = getenv("ENV", "local")
MAX_CONNECTIONS = int(getenv("MAX_CONNECTIONS", "5"))
QUERY_RETRY_LIMIT = int(getenv("QUERY_RETRY_LIMIT", 3))
MIN_SEM_WAIT = int(getenv("MIN_WAIT", "1"))

VANTAGE_COOLDOWN = int(getenv("VANTAGE_COOLDOWN", "60"))
VANTAGE_SEMAPHORE_LIMIT = int(getenv("VANTAGE_SEMAPHORE_LIMIT", "5"))
VERBOSE = int(getenv("VERBOSE", "2"))

# Discontinued symbols (ignored during data retrieval)
SYMBOLS_TO_IGNORE = ["ABE.MDR"]

if ENV == "local":
    LOG_LEVEL = logging.DEBUG
elif ENV == "prod":
    LOG_LEVEL = logging.INFO

HEADERS = {
    'user-agent': ('Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_5) '
                   'AppleWebKit/537.36 (KHTML, like Gecko) '
                   'Chrome/45.0.2454.101 Safari/537.36'),
}


stock_parameters = {
    "symbol": "Symbol",
    "currency": "Currency",
    "name": "Name",
    "FirstTimeStamp": "FirstTimeStamp",
    "Last Refreshed": "LastUpdate",
    "type": "Type",
    "region": "Region",
    "Time Zone": "TimeZone",
    }

fx_parameters = {
    "FirstTimeStamp": "FirstTimeStamp",
    "Last Refreshed": "LastUpdate",
    "Time Zone": "TimeZone",
    "Information": "Information"}

crypto_parameters = {
    "FirstTimeStamp": "FirstTimeStamp",
    "Last Refreshed": "LastUpdate",
    "Time Zone": "TimeZone",
    "Information": "Information"}

__FILE_KEYS = ROOT.joinpath("keys.yml")
__FILE_CURRENCIES = DATA_FOLDER.joinpath("currencies.yml")


def load_yml(ref):
    if ref.exists():
        with open(ref, mode="r") as f:
            return yaml.load(f, Loader=yaml.FullLoader)
    else:
        raise Exception("ERROR: File not found")


def load_keys(ref):
    try:
        return load_yml(ref)
    except Exception as _:
        raise Exception("ERROR: Please create the file 'keys.yml' at the root of the repository with valid keys")


KEYS_SET = load_keys(__FILE_KEYS)
CURRENCIES_INFO = load_yml(__FILE_CURRENCIES)

fx_currencies = [key for key, val in CURRENCIES_INFO.items() if val["type"] == "currency"]
crypto_currencies = [key for key, val in CURRENCIES_INFO.items() if val["type"] == "cryptocurrency"]
