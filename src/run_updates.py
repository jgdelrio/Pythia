import re
from src.config import DATA_FOLDER, crypto_currencies
from src.api_manager import retrieve_stock_list



currency_regex = re.compile(r"\A[A-Z]{3}_[A-Z]{3}")
FX_UPDATES = (
    ["GBP", "EUR"],
    ["GBP", "USD"],
    ["GBP", "CNY"],
    ["GBP", "INR"],
)


def get_all_stock_references():
    """Return list of existing stocks"""
    stock_folders = [x for x in DATA_FOLDER.iterdir() if x.is_dir() and "_" not in x.name]
    return [x.name for x in stock_folders]


def get_all_fx_references():
    """Return list of existing currencies"""
    fx_refs = [x.name for x in DATA_FOLDER.iterdir() if x.is_dir() and currency_regex.match(x.name)]
    return [fx for fx in fx_refs if fx not in crypto_currencies]


def get_all_crypto_references():
    """Return list of existing cryptocurrencies"""
    crypto_refs = [x.name for x in DATA_FOLDER.iterdir() if x.is_dir() and currency_regex.match(x.name)]
    return [cryp for cryp in crypto_refs if cryp in crypto_currencies]


def update_all_stock_data(gap=7):
    """Get all existing stocks and update their info"""
    symbols = get_all_stock_references()
    retrieve_stock_list(symbols, category="daily", gap=gap)
    retrieve_stock_list(symbols, category="monthly", gap=gap)


def update_all_fx_data(fx_pairs=None, gap=7):
    """Get all existing currencies and update their info"""
    if fx_pairs is None:
        fx_pairs = get_all_fx_references()
    retrieve_stock_list(fx_pairs, category="fx_daily", gap=gap)
    retrieve_stock_list(fx_pairs, category="fx_monthly", gap=gap)


def update_all_crypto_data(crypto_pairs=None, gap=7):
    """Get all existing cryptocurrencies and update their info"""
    if crypto_pairs is None:
        crypto_pairs = get_all_crypto_references()
    retrieve_stock_list(crypto_pairs, category="digital_daily", gap=gap)
    retrieve_stock_list(crypto_pairs, category="digital_monthly", gap=gap)


def update_all(gap=7):
    update_all_stock_data(gap=gap)
    update_all_fx_data(gap=gap)
    update_all_crypto_data(gap=gap)


if __name__ == "__main__":
    crypto_pairs = [["BTC", "GBP"], ["ETH", "GBP"], ["USDT", "GBP"], ["XRP", "GBP"]]
    update_all_crypto_data(crypto_pairs)
