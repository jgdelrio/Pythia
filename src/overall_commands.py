import re
import pandas as pd
from src.config import DATA_FOLDER, DFT_CRIPTO_PREFIX, DFT_INFO_FILE, DFT_INFO_EXT, VERBOSE
from src.api_manager import gather_info, retrieve_stock_list
from src.utils import LOG


currency_regex = re.compile(r"\A[A-Z]{3}_[A-Z]{3}")
crypto_regex = re.compile(r"\A" + DFT_CRIPTO_PREFIX + r"[A-Z]{3,4}_[A-Z]{3}")
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
    stock_folders = [x for x in DATA_FOLDER.iterdir() if x.is_dir() and "_" not in x.name]
    stock_folders.sort()
    return stock_folders


def get_stock_references():
    """Return list of existing stocks"""
    stock_folders = get_stock_folders()
    stock_names = [x.name for x in stock_folders]
    stock_names.sort()
    return stock_names, stock_folders


def get_fx_folders():
    """Return list of folders for physical and digital currencies"""
    fx_folders = [x for x in DATA_FOLDER.iterdir() if x.is_dir() and currency_regex.match(x.name)]
    fx_folders.sort()
    return fx_folders


def get_crypto_folders():
    """Return list of folders for physical and digital currencies"""
    crytp_folders = [x for x in DATA_FOLDER.iterdir() if x.is_dir() and crypto_regex.match(x.name)]
    crytp_folders.sort()
    return crytp_folders


def get_fx_references():
    """Return list of existing currencies"""
    fx_folders = get_fx_folders()
    fx_names = [fx.name for fx in fx_folders]
    fx_names.sort()
    return fx_names, fx_folders


def get_crypto_references():
    """Return list of existing cryptocurrencies"""
    crypto_folders = get_crypto_folders()
    crypto_names = [cryp.name for cryp in get_fx_folders()]
    crypto_names.sort()
    return crypto_names, crypto_folders


def update_all_stock_data(stocks=None, gap=7):
    """Get all existing stocks and update their info"""
    if stocks is None:
        stocks, _ = get_stock_references()
    validate_list(stocks)
    retrieve_stock_list(stocks, category="daily", gap=gap)
    retrieve_stock_list(stocks, category="monthly", gap=gap)
    retrieve_stock_list(stocks, category="daily_adjusted", gap=gap)
    retrieve_stock_list(stocks, category="monthly_adjusted", gap=gap)
    LOG.info("Stocks update finished!")


def update_all_fx_data(fx_pairs=None, gap=7):
    """Get all existing currencies and update their info"""
    if fx_pairs is None:
        fx_pairs, _ = get_fx_references()
    validate_list(fx_pairs)
    retrieve_stock_list(fx_pairs, category="fx_daily", gap=gap)
    retrieve_stock_list(fx_pairs, category="fx_monthly", gap=gap)
    LOG.info("FX update finished!")


def update_all_crypto_data(crypto_pairs=None, gap=7):
    """Get all existing cryptocurrencies and update their info"""
    if crypto_pairs is None:
        crypto_pairs, _ = get_crypto_references()
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


def map_field(array, field):
    return list(map(lambda x: x.get(field, None) if x else None, array))


def get_fx_table(mode="fx", verbose=VERBOSE):
    if mode == "fx":
        id_refs, id_folders = get_fx_references()
    elif mode == "crypto":
        id_refs, id_folders = get_fx_references()
        id_refs = id_refs[5:]

    return __create_table(id_refs, id_folders)


def __create_table(id_refs, id_folders):
    # Full path ref to the info files
    info_refs = [ref.joinpath(DFT_INFO_FILE + DFT_INFO_EXT) for ref in id_folders]
    # Read info
    fx_info = gather_info(info_refs, verbose=verbose)

    # Build columns
    from_fx, to_fx = zip(*map(lambda x: x.split("_"), id_refs))

    table = pd.DataFrame({"From": from_fx,
                          "To": to_fx,
                          "FirstTimeStamp": map_field(fx_info, "FirstTimeStamp"),
                          "LastUpdate": map_field(fx_info, "Refreshed"),
                          "TimeZone": map_field(fx_info, "Zone"),
                          "Information": map_field(fx_info, "Information")})
    return table


def get_crypto_table(verbose=VERBOSE):
    fx_refs, fx_folders = get_crypto_references()
    # Full path ref to the info files
    info_refs = [ref.joinpath(DFT_INFO_FILE + DFT_INFO_EXT) for ref in fx_folders]
    # Read info
    fx_info = gather_info(info_refs, verbose=verbose)

    # Build columns
    from_fx, to_fx = zip(*map(lambda x: x.split("_"), fx_refs))

    table = pd.DataFrame({"From": from_fx,
                          "To": to_fx,
                          "FirstTimeStamp": map_field(fx_info, "FirstTimeStamp"),
                          "LastUpdate": map_field(fx_info, "Refreshed"),
                          "TimeZone": map_field(fx_info, "Zone"),
                          "Information": map_field(fx_info, "Information")})
    return table


def get_data_table():
    # TODO: create table with a classification of existing data
    """
    Generates a table with existing symbols, classification, country, currency, folder...
    :return: table
    """


    table = pd.DataFrame()


if __name__ == "__main__":
    # stocks = ['BAS.DEX']
    # retrieve_stock_list(stocks)
    get_currency_table(verbose=3)
    update_all_stock_data(gap=2)
