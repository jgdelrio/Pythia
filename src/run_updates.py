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


def validate(sym):
    if not isinstance(sym, (tuple, list)):
        raise TypeError("The parameter must be a list or tuple")


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


def update_all_stock_data(stocks=None, gap=7):
    """Get all existing stocks and update their info"""
    if stocks is None:
        stocks = get_all_stock_references()
    validate(stocks)
    retrieve_stock_list(stocks, category="daily", gap=gap)
    retrieve_stock_list(stocks, category="monthly", gap=gap)
    retrieve_stock_list(stocks, category="daily_adjusted", gap=gap)
    retrieve_stock_list(stocks, category="monthly_adjusted", gap=gap)


def update_all_fx_data(fx_pairs=None, gap=7):
    """Get all existing currencies and update their info"""
    if fx_pairs is None:
        fx_pairs = get_all_fx_references()
    validate(fx_pairs)
    retrieve_stock_list(fx_pairs, category="fx_daily", gap=gap)
    retrieve_stock_list(fx_pairs, category="fx_monthly", gap=gap)


def update_all_crypto_data(crypto_pairs=None, gap=7):
    """Get all existing cryptocurrencies and update their info"""
    if crypto_pairs is None:
        crypto_pairs = get_all_crypto_references()
    validate(crypto_pairs)
    retrieve_stock_list(crypto_pairs, category="digital_daily", gap=gap)
    retrieve_stock_list(crypto_pairs, category="digital_monthly", gap=gap)


def update_all(gap=7):
    update_all_stock_data(gap=gap)
    update_all_fx_data(gap=gap)
    update_all_crypto_data(gap=gap)


def test_update_crypto():
    crypto_pairs = [["BTC", "GBP"], ["ETH", "GBP"], ["USDT", "GBP"], ["XRP", "GBP"]]
    update_all_crypto_data(crypto_pairs)


if __name__ == "__main__":
    # stocks = ['MMM']
    update_all_stock_data(gap=2)
