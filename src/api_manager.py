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
import pandas as pd
import traceback
from datetime import datetime
from src.alpha_vantage_api import alpha_vantage_query, fx_regex
from src.utils import LOG
from src.config import DATA_FOLDER, KEYS_SET, HEADERS, MAX_CONNECTIONS, MIN_SEM_WAIT, \
    DFT_STOCK_FILE, DFT_STOCK_EXT, DFT_INFO_FILE, DFT_INFO_EXT, DFT_FX_FILE, DFT_FX_EXT, VERBOSE


nest_asyncio.apply()
# RegExp
clean_names_regex = re.compile("[\w]*$")
capture_enum_regex = re.compile("^[\d]*\.\s*")
# Functions & Filtering
dateparse = lambda dates: pd.datetime.strptime(dates, '%Y-%m-%d')


def build_path_and_file(symbol, category):
    if isinstance(symbol, (list, tuple)):
        # FX currencies (from, to):
        folder_name = DATA_FOLDER.joinpath(symbol[0] + "_" + symbol[1])
        file_name = folder_name.joinpath(DFT_FX_FILE + "_" + category + DFT_FX_EXT)
    else:
        folder_name = DATA_FOLDER.joinpath(symbol)
        file_name = folder_name.joinpath(DFT_STOCK_FILE + "_" + category + DFT_STOCK_EXT)

    folder_name.mkdir(parents=True, exist_ok=True)      # Create if doesn't exist
    return folder_name, file_name


def build_info_file(folder_name, category):
    return folder_name.joinpath(DFT_INFO_FILE + "_" + category + DFT_INFO_EXT)


async def query_data(symbol, semaphore, category=None, api="vantage", verbose=VERBOSE):
    if category is None:
        raise ValueError("Please provide a valid category in the parameters")
    await semaphore.acquire()
    if verbose > 2:
        LOG.info("Successfully acquired the semaphore")

    if api == "vantage":
        url, params = alpha_vantage_query(symbol, category, key=KEYS_SET["alpha_vantage"])
        LOG.info(f"Retrieving {symbol} from '{api}'")

    async with aiohttp.ClientSession() as session:
        async with session.get(url, params=params, headers=HEADERS) as resp:
            data = await resp.json()

    if "Error Message" in data.keys():
        LOG.error(f"ERROR: Not possible to retrieve {symbol}. Msg: {data['Error Message']}")
        semaphore.release()
        return None

    await asyncio.sleep(MIN_SEM_WAIT)
    if verbose > 2:
        LOG.info("Releasing Semaphore")
    semaphore.release()
    return data


def process_vantage_data(data):
    metadata = data.get("Meta Data", None)
    if metadata:
        try:
            info = {key.replace(capture_enum_regex.findall(key)[0], ""): val for key, val in metadata.items()}
        except Exception as err:
            LOG.ERROR("ERROR cleaning info")
            print(metadata)
    else:
        info = {}
    data_key = [k for k in data.keys() if k != "Meta Data"][0]      # 'Time Series (Daily)' or 'Time Series FX (Weekly)'
    dat = data[data_key]
    return info, dat


async def save_stock_info(info_file, info, old_info=None):
    into2write = info if old_info is None else {**old_info, **info}     # Select new or merge

    async with aiofiles.open(info_file.as_posix(), mode="w+") as f:
        await f.write(json.dumps(into2write, indent=2).encode('ascii', 'ignore').decode('ascii'))


async def read_stock_info(info_file, check=True):
    if check:
        if not info_file.exists():
            raise ValueError(f"ERROR: No info found at {info_file}")

    if info_file.exists():
        async with aiofiles.open(info_file, "r") as f:
            return json.load(f)
    else:
        return {}


async def update_stock_info(info_file, info, verbose=VERBOSE):
    try:
        # Clean key names
        clean_info = {}
        for key, val in info.items():
            try:
                key = key.replace(capture_enum_regex.findall(key)[0], "")
            except Exception:
                pass
            clean_info[key] = val

        clean_info.pop('matchScore', None)
        old_info = await read_stock_info(info_file, check=False)
        await save_stock_info(info_file, clean_info, old_info=old_info)
        if verbose > 1:
            symbol = info_file.parent
            tabs = "\t" * 2 if len(symbol) <= 5 else "\t"
            LOG.info(f"Updating {symbol} info:{tabs}OK")
    except Exception as err:
        LOG.error(f"ERROR updating info: {info_file}. Msg: {err.__repr__()} {traceback.print_tb(err.__traceback__)}")


def save_pandas_data(file_name, data, verbose=VERBOSE):
    try:
        stock_data = pd.DataFrame.from_dict(data, orient="index")
        # Apply clean names to columns and index
        column_names = [clean_names_regex.findall(c)[0] for c in stock_data.columns.tolist()]
        stock_data.columns = column_names
        stock_data.index.name = 'date'

        stock_data.sort_index(axis=0, inplace=True, ascending=True)                     # Sort by date
        stock_data.reset_index().to_csv(file_name, index=False, compression="infer")    # Save
        if verbose > 1:
            LOG.info(f"Pandas data {file_name} saved OK")
    except Exception as err:
        LOG.error(f"ERROR saving pandas data: {file_name}")


def read_pandas_data(file_name):
    if not file_name.exists():
        LOG.error(f"ERROR: data not found for {file_name}")
        return None
    return pd.read_csv(file_name, parse_dates=['date'], index_col='date', date_parser=dateparse)


async def update_stock(symbol, semaphore, category="daily", max_gap=0, api="vantage"):
    folder_name, file_name = build_path_and_file(symbol, category)
    info_file = build_info_file(folder_name, category)
    tabs = "\t" * 2 if len(symbol) <= 5 else "\t"
    info = None

    try:
        if folder_name.exists() and file_name.exists():
            # Verify how much must be updated
            data_stored = read_pandas_data(file_name)
            delta = (datetime.now() - data_stored.index[-1]).days

            if delta > max_gap:
                # Retrieve 'delta' days
                # TODO: retrieve only specific date range
                data = await query_data(symbol, semaphore, category=category, api="vantage")
                info, dat = process_vantage_data(data)

                LOG.info(f"Updating {symbol} stock data")
                save_pandas_data(file_name, dat)
            else:
                LOG.info(f"Updating {symbol}:{tabs}Ignored. Data available is < {max_gap} days old")
                return
        else:
            # Download and save new data
            data = await query_data(symbol, semaphore, category=category, api=api)
            if data is None:
                return
            info, dat = process_vantage_data(data)
            save_pandas_data(file_name, dat)

        # Save/Update info
        if info:
            await update_stock_info(info_file, info)

        LOG.info(f"Updating {symbol}:{tabs}OK")
    except Exception as err:
        LOG.info(f"Updating {symbol}:{tabs}ERROR: {err.__repr__()} {traceback.print_tb(err.__traceback__)}")


def retrieve_stock_list(symbols, category="daily", gap=7, api="vantage", limit=MAX_CONNECTIONS):
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
    sem = asyncio.Semaphore(value=limit)
    loop = asyncio.get_event_loop()
    loop.run_until_complete(
        asyncio.gather(
            *(update_stock(symbol, sem, category=category, max_gap=gap, api=api) for symbol in symbols)
        )
    )


def search_symbol(symbols, api="vantage", limit=MAX_CONNECTIONS, verbose=VERBOSE):
    if isinstance(symbols, str):
        symbols = [symbols]

    sem = asyncio.Semaphore(value=limit)
    loop = asyncio.get_event_loop()
    return loop.run_until_complete(
        asyncio.gather(
            *(query_data(symbol, sem, category="search", api=api, verbose=verbose) for symbol in symbols)
        )
    )


def update_info_with_search(symbols=None, verbose=VERBOSE):
    if symbols is None:
        # Update existing folders (except currencies)
        stock_folders = [x for x in DATA_FOLDER.iterdir() if x.is_dir() and "_" not in x.name]
        symbols = [x.name for x in stock_folders]

    # Search symbols
    grp_info = search_symbol(symbols, api="vantage", verbose=verbose)
    grp_info = [data['bestMatches'][0] for data in grp_info]

    # Build folders list (and create if they don't exist) and file list
    if "stock_folders" not in locals():
        stock_folders, _ = list(zip(*[build_path_and_file(symbol, "any") for symbol in symbols]))
    info_files = [build_info_file(folder, "daily") for folder in stock_folders]

    # Update info
    loop = asyncio.get_event_loop()
    loop.run_until_complete(
        asyncio.gather(*(update_stock_info(info_f, info) for info_f, info in zip(info_files, grp_info)))
    )
    return grp_info


def test_search_symbol():
    symbols = ["BASF", "Diageo", "Gilead", "Johnson&Johnson"]
    result = search_symbol(symbols)
    print(result)


def test_retrieve_stocks():
    symbols = ["AMAT", "AMZN", "ATVI", "GOOG", "MMM", "RNSDF", "XOM"]
    # ISA: MMM, Blizzard, Alphabet, Applied materias, BASF Diageo, Gilead Johnson&Johnson, Judges Scientific, Nvidia, Pfizer, Rio Tinto, SSE, Walt Disney
    # SIPP: Altria, Amazon, Axa, BHP, BT, Dassault Systemes, Henkel AG&CO, Liberty Global, National Grid, Reach PLC, Renault, Sartorius AG, Starbucks
    problematic = ["RDS.A"]

    retrieve_stock_list(symbols, category="daily", gap=7)


if __name__ == "__main__":
    test_retrieve_stocks()
