import re
import pandas as pd
from src.config import DATA_FOLDER, DFT_CRIPTO_PREFIX, DFT_INFO_FILE, DFT_INFO_EXT, VERBOSE
from src.api_manager import gather_info, retrieve_stock_list
from src.utils import LOG


currency_regex = re.compile(r"\A[A-Z]{3}_[A-Z]{3}")
crypto_regex = re.compile(r"\A" + DFT_CRIPTO_PREFIX + r"[A-Z]{3,4}_[A-Z]{3}")
info_data_pattern = DFT_INFO_FILE + r"*" + DFT_INFO_EXT
FX_UPDATES = (
    ["GBP", "EUR"],
    ["GBP", "USD"],
    ["GBP", "CNY"],
    ["GBP", "INR"],
)

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

share_parameters = {"symbol": "Symbol",
                    "currency": "Currency",
                    "name": "Name",
                    "type": "Type",
                    "region": "Region",
                    "FirstTimeStamp": "FirstTimeStamp",
                    "Last Refreshed": "LastUpdate",
                    "timezone": "TimeZone",
                    }


def validate_list(sym):
    if not isinstance(sym, (tuple, list)):
        raise TypeError("The parameter must be a list or tuple")


def get_stock_folders():
    """Return list of existing stock folders"""
    stock_folders = [x for x in DATA_FOLDER.iterdir() if x.is_dir() and "_" not in x.name]
    stock_folders.sort()
    return stock_folders


def get_share_references():
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
    fx_names = [fx.split("_") for fx in fx_names]
    return fx_names, fx_folders


def get_crypto_references():
    """Return list of existing cryptocurrencies in pairs (from to)"""
    crypto_folders = get_crypto_folders()
    crypto_names = [cryp.name for cryp in crypto_folders]
    crypto_names.sort()
    crypto_names = [cr.replace("CRYPTO_", "").split("_") for cr in crypto_names]
    return crypto_names, crypto_folders


def update_all_stock_data(stocks=None, gap=7, verbose=VERBOSE):
    """Get all existing stocks and update their info"""
    if stocks is None:
        stocks, _ = get_share_references()
    validate_list(stocks)
    retrieve_stock_list(stocks, category="daily", gap=gap, verbose=verbose)
    retrieve_stock_list(stocks, category="monthly", gap=gap, verbose=verbose)
    retrieve_stock_list(stocks, category="daily-adjusted", gap=gap, verbose=verbose)
    retrieve_stock_list(stocks, category="monthly-adjusted", gap=gap, verbose=verbose)
    LOG.info("Stocks update finished!")


def update_all_fx_data(fx_pairs=None, gap=7):
    """Get all existing currencies and update their info"""
    if fx_pairs is None:
        fx_pairs, _ = get_fx_references()
    validate_list(fx_pairs)
    validate_list(fx_pairs[0])
    retrieve_stock_list(fx_pairs, category="fx_daily", gap=gap)
    retrieve_stock_list(fx_pairs, category="fx_monthly", gap=gap)
    LOG.info("FX update finished!")


def update_all_crypto_data(crypto_pairs=None, gap=7):
    """Get all existing cryptocurrencies and update their info"""
    if crypto_pairs is None:
        crypto_pairs, _ = get_crypto_references()
    validate_list(crypto_pairs)
    validate_list(crypto_pairs[0])
    retrieve_stock_list(crypto_pairs, category="digital_daily", gap=gap)
    retrieve_stock_list(crypto_pairs, category="digital_monthly", gap=gap)
    LOG.info("Crypto update finished!")


def update_all(gap=3):
    update_all_stock_data(gap=gap)
    update_all_fx_data(gap=gap)
    update_all_crypto_data(gap=gap)


def test_update_crypto():
    crypto_pairs = [["BTC", "GBP"], ["ETH", "GBP"], ["USDT", "GBP"], ["XRP", "GBP"]]
    update_all_crypto_data(crypto_pairs)


def map_field(array, field):
    return [x.get(field, None) for x in array]


def get_fx_table(mode="fx", verbose=VERBOSE):
    if mode == "fx":
        id_refs, id_folders = get_fx_references()
    elif mode == "crypto":
        id_refs, id_folders = get_crypto_references()
        id_refs = [k[len(DFT_CRIPTO_PREFIX):] for k in id_refs]
    return __create_table(zip(id_folders, id_refs), variant=mode, verbose=verbose)


def __create_table(refs, variant="fx", verbose=VERBOSE):
    rows = []
    for id_folder, id_refs in refs:
        if isinstance(id_refs, (list, tuple)):
            from_fx, to_fx = id_refs
        else:
            from_fx, to_fx = id_refs.split("_")
        info_files = list(id_folder.glob(info_data_pattern))

        if info_files:
            for info_ref in info_files:
                rows.append((from_fx, to_fx, info_ref))
        else:
            rows.append((from_fx, to_fx, None))

    if variant == "fx":
        parameters = fx_parameters
    elif variant == "crypto":
        parameters = crypto_parameters

    # Read info from files
    info = gather_info(map(lambda x: x[2], rows), verbose=verbose)
    table_dict = {"From": list(map(lambda x: x[0], rows)),
                  "To": list(map(lambda x: x[1], rows)),
                  "Period": list(map(lambda x: x[2].stem.split("_")[-1], rows)),
                  **{val: map_field(info, key) for key, val in parameters.items()}}

    table = pd.DataFrame(table_dict)
    return table


def get_shares_table(verbose=0):
    """
    Generates a table with existing symbols, classification, country, currency, folder...
    :return: table
    """

    rows = []
    for share, id_folder in zip(*get_share_references()):
        info_files = list(id_folder.glob(info_data_pattern))

        if info_files:
            for info_ref in info_files:
                rows.append((share, info_ref))
        else:
            rows.append((share, None))

    # Read info from files
    info = gather_info(map(lambda x: x[1], rows), verbose=verbose)
    table_dict = {
                  **{val: map_field(info, key) for key, val in share_parameters.items()}}

    table = pd.DataFrame(table_dict)
    return table


if __name__ == "__main__":
    # stocks = ['BAS.DEX']
    # retrieve_stock_list(stocks)

    # print(get_share_table(verbose=3))
    # print(get_fx_table(mode="fx", verbose=3))
    # print(get_fx_table(mode="crypto", verbose=3))

    # update_all(gap=2)
    get_fx_table()