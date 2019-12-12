import re
from src.config import DATA_FOLDER, crypto_currencies
from src.api_manager import retrieve_stock_list
from src.utils import LOG


currency_regex = re.compile(r"\A[A-Z]{3}_[A-Z]{3}")
FX_UPDATES = (
    ["GBP", "EUR"],
    ["GBP", "USD"],
    ["GBP", "CNY"],
    ["GBP", "INR"],
)


def validate_list(sym):
    if not isinstance(sym, (tuple, list)):
        raise TypeError("The parameter must be a list or tuple")


def get_stock_folders():
    """Return list of existing stock folders"""
    return [x for x in DATA_FOLDER.iterdir() if x.is_dir() and "_" not in x.name]


def get_all_stock_references():
    """Return list of existing stocks"""
    stock_names = [x.name for x in get_stock_folders()]
    stock_names.sort()
    return stock_names


def get_fx_folders():
    """Return list of folders for physical and digital currencies"""
    return [x.name for x in DATA_FOLDER.iterdir() if x.is_dir() and currency_regex.match(x.name)]


def get_all_fx_references():
    """Return list of existing currencies"""
    fx_names = [fx for fx in get_fx_folders() if fx not in crypto_currencies]
    fx_names.sort()
    return fx_names


def get_all_crypto_references():
    """Return list of existing cryptocurrencies"""
    crypto_names = [cryp for cryp in get_fx_folders() if cryp in crypto_currencies]
    crypto_names.sort()
    return crypto_names


def update_all_stock_data(stocks=None, gap=7):
    """Get all existing stocks and update their info"""
    if stocks is None:
        stocks = get_all_stock_references()
    validate_list(stocks)
    retrieve_stock_list(stocks, category="daily", gap=gap)
    retrieve_stock_list(stocks, category="monthly", gap=gap)
    retrieve_stock_list(stocks, category="daily_adjusted", gap=gap)
    retrieve_stock_list(stocks, category="monthly_adjusted", gap=gap)
    LOG.info("Stocks update finished!")


def update_all_fx_data(fx_pairs=None, gap=7):
    """Get all existing currencies and update their info"""
    if fx_pairs is None:
        fx_pairs = get_all_fx_references()
    validate_list(fx_pairs)
    retrieve_stock_list(fx_pairs, category="fx_daily", gap=gap)
    retrieve_stock_list(fx_pairs, category="fx_monthly", gap=gap)
    LOG.info("FX update finished!")


def update_all_crypto_data(crypto_pairs=None, gap=7):
    """Get all existing cryptocurrencies and update their info"""
    if crypto_pairs is None:
        crypto_pairs = get_all_crypto_references()
    validate_list(crypto_pairs)
    retrieve_stock_list(crypto_pairs, category="digital_daily", gap=gap)
    retrieve_stock_list(crypto_pairs, category="digital_monthly", gap=gap)
    LOG.info("Crypto update finished!")


def update_all(gap=7):
    update_all_stock_data(gap=gap)
    update_all_fx_data(gap=gap)
    update_all_crypto_data(gap=gap)


def test_update_crypto():
    crypto_pairs = [["BTC", "GBP"], ["ETH", "GBP"], ["USDT", "GBP"], ["XRP", "GBP"]]
    update_all_crypto_data(crypto_pairs)


def get_data_table():
    # TODO: create table with a classification of existing data
    """
    Generates a table with existing symbols, classification, country, currency, folder...
    :return: table
    """
    pass


if __name__ == "__main__":
    # stocks = ['BAS.DEX']
    # retrieve_stock_list(stocks)

    update_all_stock_data(gap=2)
