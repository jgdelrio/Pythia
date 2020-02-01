"""
#########################   API Manager   #########################
Retrieves information of stocks and currency using multiple API's
###################################################################
"""
import re
import json
import aiohttp
import asyncio
import aiofiles
import nest_asyncio
import traceback
import pandas as pd
from datetime import datetime, timedelta

from src.config import *
from src.crawler_semaphore import SemaphoreController
from src.alpha_vantage_api import alpha_vantage_query, manage_vantage_errors
from src.utils import LOG, get_tabs, get_index, map_field, add_first_ts, read_pandas_data, save_pandas_data, \
    clean_enumeration, transform_column_types, dict2pandas_data, compare_dfs_by_index, check_monotonic_ts_index


nest_asyncio.apply()
# RegExp
clean_names_regex = re.compile("[\w]*$")
# Functions & Filtering
semaphore_controller = SemaphoreController()


def build_path_and_file(symbol, category):
    if isinstance(symbol, (list, tuple)):
        # FX currencies (from, to) and digital currencies:
        if "digital_" in category:
            subfolder = "CRYPTO_" + symbol[0].upper() + "_" + symbol[1].upper()
        else:
            subfolder = symbol[0].upper() + "_" + symbol[1].upper()
        folder_name = DATA_FOLDER.joinpath(subfolder)
        file_name = folder_name.joinpath(DFT_FX_FILE + "_" + category + DFT_FX_EXT)
    else:
        # Shares & stocks
        folder_name = DATA_FOLDER.joinpath(symbol.upper())
        file_name = folder_name.joinpath(DFT_STOCK_FILE + "_" + category + DFT_STOCK_EXT)

    folder_name.mkdir(parents=True, exist_ok=True)      # Create if doesn't exist
    return folder_name, file_name


def delta_now_surpassed(last_date, max_gap, category):
    now = datetime.now()
    delta = (now - last_date).days

    if delta > max_gap:
        if "monthly" in category and ((now.month - last_date.month > 0) or (now.year - last_date.year > 0)):
            return True
        elif "weekly" in category and delta > 7:
            return True
        else:
            return True
    return False


def build_info_file(folder_name, category):
    return folder_name.joinpath(DFT_INFO_FILE + "_" + category + DFT_INFO_EXT)


async def query_data(symbol, category=None, api="vantage", verbose=VERBOSE, **kwargs):
    if category is None:
        raise ValueError("Please provide a valid category in the parameters")
    # Get semaphore
    await semaphore_controller.get_semaphore(api)

    if verbose > 2:
        LOG.info("Successfully acquired the semaphore")

    if api == "vantage":
        url, params = alpha_vantage_query(symbol, category, key=KEYS_SET["alpha_vantage"], **kwargs)
        LOG.info("Retrieving {}:{}From '{}' API".format(symbol, get_tabs(symbol, prev=12), api))
    elif api == "quandl":
        key = KEYS_SET["quandl"]
    else:
        LOG.error("Not supported api {}".format(api))

    counter = 0
    while counter <= QUERY_RETRY_LIMIT:
        async with aiohttp.ClientSession() as session:
            async with session.get(url, params=params, headers=HEADERS) as resp:
                data = await resp.json()

        if api == "vantage":
            checked_response = manage_vantage_errors(data, symbol)
            if checked_response == "longWait":
                counter += 1
                await asyncio.sleep(VANTAGE_COOLDOWN)
            elif checked_response == "api_error":
                data = None
            else:
                break

    await asyncio.sleep(MIN_SEM_WAIT)
    if verbose > 2:
        LOG.info("Releasing Semaphore")
    # Release semaphore
    semaphore_controller.release_semaphore(api)
    return data


def process_vantage_data(data):
    """Receives the data as a dictionary of info + values and return the two independent dictionaries"""
    metadata = data.get("Meta Data", None)
    if metadata:
        try:
            info = clean_enumeration(metadata)
        except Exception as err:
            LOG.ERROR("ERROR cleaning info: {}".format(metadata))
            info = metadata
    else:
        info = {}
    data_key = [k for k in data.keys() if k != "Meta Data"][0]      # 'Time Series (Daily)' or 'Time Series FX (Weekly)'
    dat = data[data_key]
    return info, dat


def clean_info(info, file_ref):
    file_name = file_ref.stem
    prop_map = fx_parameters if any([k in file_name for k in ["digital", "fx"]]) else stock_parameters
    return {prop_map.get(key, key): val for key, val in info.items()}


async def save_stock_info(info_file, info, old_info=None, create=True):
    """Save/overwrite info data if previous exists or create is True"""
    info2write = info if old_info is None else {**old_info, **info}     # Select new or merge
    info2write = clean_info(info2write, info_file)

    write = True if (info_file.exists() or create) else False
    if write:
        async with aiofiles.open(info_file.as_posix(), mode="w") as file_ref:
            await file_ref.write(json.dumps(info2write, indent=2).encode('ascii', 'ignore').decode('ascii'))


async def read_info_file(info_file, check=True, verbose=VERBOSE):
    if not info_file:
        return {}
    if info_file.exists():
        async with aiofiles.open(info_file, "r") as info:
            data = await info.read()
            if verbose > 1:
                LOG.info("Info file read:{}{}".format(get_tabs('', prev=15), info_file))
            return json.loads(data)
    else:
        if check:
            LOG.error("ERROR: No info found at {}".format(info_file))
        if verbose > 1:
            LOG.warning("Info file: {}\tDO NOT EXISTS!".format(info_file))
        return {}


async def update_stock_info(info_file, info, create=True, verbose=VERBOSE):
    try:
        # Clean key names
        clean_info = clean_enumeration(info)
        clean_info.pop('matchScore', None)

        # Read previous info
        if info_file.exists():
            old_info = await read_info_file(info_file, check=False, verbose=verbose)
        else:
            old_info = {}

        await save_stock_info(info_file, clean_info, old_info=old_info, create=create)
        if verbose > 1:
            symbol = info_file.parent.name
            LOG.info("Updating {} info:{}OK".format(symbol, get_tabs(symbol, prev=15)))
    except Exception as err:
        LOG.error("ERROR updating info: {}. Msg: {} {}".format(info_file, err.__repr__(),
                                                               traceback.print_tb(err.__traceback__)))


def load_stock_data(symbols, period="daily", date_ini=None, date_end=None):
    unique_value = False
    if period not in INFO_VARIATIONS:
        raise ValueError("The period {} is not supported. Please select one among {}".format(period, INFO_VARIATIONS))
    if isinstance(symbols, str):
        unique_value = True
        symbols = [symbols]

    folders, files = zip(*[build_path_and_file(symbol, period) for symbol in symbols])
    data_group = []
    for file_name in files:
        # Read and transform data types
        data = transform_column_types(read_pandas_data(file_name))

        if date_ini:
            data = data[data.index >= date_ini]
        if date_end:
            data = data[data.index <= date_end]
        data_group.append(data)

    if unique_value:
        return data_group[0]
    else:
        return data_group


def get_latest_stock_data(symbol, period="daily"):
    data = load_stock_data(symbol, period=period)
    return data.iloc[-1, :]                             # Return latest available


def get_dividends(symbol, date_ini=None, date_end=None):
    """
    Retrieve the dividends of a symbol. Total in history if no dates are specified.
    The dates are inclusive and filter the data accordingly.
    """
    # TODO
    return 0


async def update_stock(symbol, mode="stock", category="daily", max_gap=0, api="vantage", verbose=VERBOSE):
    folder_name, file_name = build_path_and_file(symbol, category)
    info_file = build_info_file(folder_name, category)
    info = None
    if mode == "stock":
        fields = STOCK_FIELDS
    elif mode == "fx":
        fields = FX_FIELDS
    elif mode == "crypto":
        fields = CRYPTO_FIELDS

    if symbol in SYMBOLS_TO_IGNORE:
        return

    try:
        if folder_name.exists() and file_name.exists():
            # Verify how much must be updated
            if mode == "crypto":
                data_stored = read_pandas_data(file_name)
            else:
                data_stored = read_pandas_data(file_name).reindex(fields, axis=1)
            check_monotonic_ts_index(data_stored, symbol, category)
            first_date = data_stored.index[0]
            last_date = data_stored.index[-1]

            if delta_now_surpassed(last_date, max_gap, category):
                LOG.info("Updating {} data...".format(symbol))
                # Retrieve only last range (alpha_vantage 100pts)
                data = await query_data(symbol, category=category, api=api, outputsize="compact")
                if data in [None, {}]:
                    LOG.WARNING("No data received for {}".format(symbol))
                    return

                if api in ["vantage"]:
                    info, dat = process_vantage_data(data)
                    data_df = dict2pandas_data(dat)                     # Transform to df
                    if mode is not "crypto":
                        data_df = data_df.reindex(fields, axis=1)       # Ensure ordered columns
                    info = add_first_ts(info, first_date)
                else:
                    raise ValueError("Invalid api {}".format(api))

                # Correct data held if necessary
                n_errors, error_ref = compare_dfs_by_index(data_stored, data_df, symbol, raiseerror=False)
                if n_errors > 0:
                    LOG.warning("Detected {} errors:{}{} {}".format(
                        n_errors, get_tabs(n_errors, prev=7), symbol, category))
                if category in ["daily", "monthly"] or "digital" in category:
                    # Non-adjusted data must be the same as what was stored
                    if n_errors > 0:
                        # Overwrite old data
                        ind_old = [k in data_df.index for k in data_stored.index]
                        ind_new = [k in data_stored.index for k in data_df.index]
                        data_stored.iloc[ind_old, :] = data_df.iloc[ind_new, :]
                    save_pandas_data(file_name, data_df, old_data=data_stored, verbose=verbose)

                elif "adjusted" in category:
                    # An event (split, dividend, etc) may have happened
                    if n_errors > 0:
                        # Verify first days
                        errors_at_beg, _ = compare_dfs_by_index(data_stored, data_df.head(3), symbol, raiseerror=False)
                        if errors_at_beg > 0:
                            # Overwrite all old data
                            data = await query_data(symbol, category=category, api=api)
                            info, dat = process_vantage_data(data)
                            data_df = dict2pandas_data(dat)                         # Transform to df
                            if mode is not "crypto":
                                data_df = data_df.reindex(fields, axis=1)           # Ensure ordered columns
                            save_pandas_data(file_name, data_df, verbose=verbose)
                        else:
                            # Overwrite only indices with errors
                            for eref in error_ref:
                                LOG.warning("Correcting {} {}:{}{}".format(
                                    symbol, category, get_tabs(symbol+category, prev=17), eref))
                                data_stored.loc[eref[1], eref[0]] = data_df.loc[eref[1], eref[0]]
                            save_pandas_data(file_name, data_df, old_data=data_stored, verbose=verbose)
            else:
                if verbose > 1:
                    LOG.info("Updating {}:{}Ignored. Data {} < {}d old".format(
                        symbol, get_tabs(symbol, prev=10), category, max_gap))
                return
        else:
            # Download and save new data
            if verbose > 1:
                LOG.info("Updating {} ...".format(symbol))
            data = await query_data(symbol, category=category, api=api)
            if data in [None, {}]:
                LOG.WARNING("No data received for {}".format(symbol))
                return

            info, dat = process_vantage_data(data)
            data_df = dict2pandas_data(dat)                         # Transform to df
            if mode is not "crypto":
                data_df = data_df.reindex(fields, axis=1)           # Ensure ordered columns
            save_pandas_data(file_name, data_df, verbose=verbose)

        # Save/Update info
        if info:
            await update_stock_info(info_file, info)

        if verbose > 1:
            LOG.info(f"Updating {symbol}:{get_tabs(symbol, prev=10)}Finished")
    except Exception as err:
        LOG.info("Updating {}:{}ERROR: {} {}".format(
            symbol, get_tabs(symbol, prev=10), err.__repr__(), traceback.print_tb(err.__traceback__)))


def retrieve_stock_list(symbols, mode="stock", category="daily", gap=7, api="vantage", verbose=VERBOSE):
    """
    Provided a list of symbols, update the info of the stocks for any stock where there is
    no information in the last (gap) days.
    :param symbols: list of symbols
    :param gap:     max allowed days of missing data before updating again
    :param api:     api used to perform the queries
    :param limit:   max number of concurrent connections
    :return:
    """
    if not isinstance(symbols, (list, tuple)):
        raise TypeError("symbols must be a list")

    if isinstance(api, list):
        tasks = (update_stock(nsymbol, mode=mode, category=category, max_gap=gap, api=napi, verbose=verbose)
                 for nsymbol, napi in zip(symbols, api))
    else:
        tasks = (update_stock(symbol, mode=mode, category=category, max_gap=gap, api=api) for symbol in symbols)

    loop = asyncio.get_event_loop()
    loop.run_until_complete(asyncio.gather(*tasks))


def search_symbol(symbols=None, api="vantage", verbose=VERBOSE):
    if symbols is None:
        print('Function Help:\n' \
              '\tProvide a list of symbols, references, possible names, ISIN ref, SEDOL ref...\n' \
              '\t\tex: ["SSE", "GB0007908733"]\n' \
              '\tFor each entry the function returns up to 10 possible matches with a score\n')
        return
    if isinstance(symbols, str):
        symbols = [symbols]

    if isinstance(api, list):
        tasks = (query_data(nsymbol, category="search", api=napi, verbose=verbose)
                 for nsymbol, napi in zip(symbols, api))
    else:
        tasks = (query_data(symbol, category="search", api=api, verbose=verbose) for symbol in symbols)

    loop = asyncio.get_event_loop()
    return loop.run_until_complete(asyncio.gather(*tasks))


def find_data(ref, db):
    idx = ref.parent.name
    data = [entry for entry in db if entry["Symbol"] == idx]
    if len(data) > 0:
        return data[0]
    else:
        LOG.warning("WARNING: Reference {} not found".format(idx))
        return {}


def update_info_with_search(symbols=None, api="vantage", verbose=VERBOSE):
    if symbols is None:
        # Update existing folders (except currencies)
        stock_folders = [x for x in DATA_FOLDER.iterdir() if x.is_dir() and "_" not in x.name]
        symbols = [x.name for x in stock_folders]

    # Load ISIN DB:
    isin_db = pd.read_csv(DATA_FOLDER.joinpath(ISIN_DB))
    isin_db = isin_db[isin_db["ISIN"] != "None"]
    # Get ISIN available
    symbols_with_ISIN = [k for k in symbols if k in isin_db.Symbol.tolist()]
    isin_db = isin_db.set_index("Symbol", verify_integrity=True)
    symbols_ISIN = isin_db.loc[symbols_with_ISIN, "ISIN"]
    missing_ISIN = [k for k in symbols if k not in symbols_with_ISIN]
    missing = pd.DataFrame({"Symbols": missing_ISIN})
    # Save symbols of missing ISINs
    missing.to_csv(DATA_FOLDER.joinpath(MISSING_DB), index=False)

    # Search symbols and get best result
    isin_info = search_symbol(symbols_ISIN.values.tolist(), api=api, verbose=verbose)
    isin_info_1st_rst = [data['bestMatches'][0] for data in isin_info]
    clean_isin_info = [clean_enumeration(k) for k in isin_info_1st_rst]
    clean_info = [{stock_parameters.get(key, key): val for key, val in item.items()} for item in clean_isin_info]

    # Build folders list (and create if they don't exist) and file list
    stock_folders, _ = list(zip(*[build_path_and_file(symbol, "any") for symbol in symbols_ISIN.index.tolist()]))
    info_files = [build_info_file(folder, variation) for folder in stock_folders for variation in INFO_VARIATIONS]

    info_groups = [(info_f, find_data(info_f, clean_info)) for info_f in info_files]
    # Clean empty groups
    info_groups_clean = [grp for grp in info_groups if not grp[1] == {}]

    # Update info
    loop = asyncio.get_event_loop()
    loop.run_until_complete(
        asyncio.gather(*(update_stock_info(file_ref, info,
                                           create=False,
                                           verbose=verbose)
                         for file_ref, info in info_groups_clean))
    )


def gather_info(files, verbose=VERBOSE):
    # Read all files requested
    loop = asyncio.get_event_loop()
    return loop.run_until_complete(asyncio.gather(*(read_info_file(fj, check=False, verbose=verbose) for fj in files)))


def test_search_symbol():
    symbols = ["BASF", "Diageo", "Gilead", "Johnson&Johnson"]
    result = search_symbol(symbols)
    print(result)


def test_retrieve_stocks():
    symbols = ["AMAT", "AMZN", "ATVI", "GOOG", "MMM", "RNSDF", "XOM"]
    # ISA: MMM, Blizzard, Alphabet, Applied materias, BASF Diageo, Gilead Johnson&Johnson, Judges Scientific, Nvidia, Pfizer, Rio Tinto, SSE, Walt Disney
    # SIPP: Altria, Amazon, Axa, BHP, BT, Dassault Systemes, Henkel AG&CO, Liberty Global, National Grid, Reach PLC, Renault, Sartorius AG, Starbucks
    # ES: ASM Lithography Holding, Bolsas y Mercados ESP, Caixabank, Naturgy Energy, Red Electrica, Endesa, Unibail-Rodamco Se And WFD Uniba
    # TODO: Indices ["ES0SI0000005", "EU0009658145", "GB0001383545", "FR0003500008"]

    retrieve_stock_list(symbols, category="daily", gap=2)


if __name__ == "__main__":
    # update_info_with_search()
    load_stock_data("BARC.LON")
    # test_retrieve_stocks()
