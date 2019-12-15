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
from dateutil.relativedelta import relativedelta
from src.alpha_vantage_api import alpha_vantage_query, manage_vantage_errors
from src.utils import LOG, get_tabs, get_index, datetime_format, end_of_week, last_day_of_month
from src.config import *


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


def delta_surpassed(last_date, max_gap, category):
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


async def query_data(symbol, semaphore, category=None, api="vantage", verbose=VERBOSE, **kwargs):
    if category is None:
        raise ValueError("Please provide a valid category in the parameters")
    await semaphore.acquire()
    if verbose > 2:
        LOG.info("Successfully acquired the semaphore")

    if api == "vantage":
        url, params = alpha_vantage_query(symbol, category, key=KEYS_SET["alpha_vantage"], **kwargs)
        LOG.info(f"Retrieving {symbol}:{get_tabs(symbol, prev=12)}From '{api}' API")
    else:
        LOG.error(f"Not supported api {api}")

    counter = 0
    while counter <= QUERY_RETRY_LIMIT:
        async with aiohttp.ClientSession() as session:
            async with session.get(url, params=params, headers=HEADERS) as resp:
                data = await resp.json()

        if api == "vantage":
            if manage_vantage_errors(data, symbol) == "longWait":
                counter += 1
                await asyncio.sleep(VANTAGE_WAIT)
            else:
                break

    await asyncio.sleep(MIN_SEM_WAIT)
    if verbose > 2:
        LOG.info("Releasing Semaphore")
    semaphore.release()
    return data


def clean_enumeration(data):
    if isinstance(data, dict):
        return {key.replace(get_index(capture_enum_regex.findall(key), 0, ""), ""): val for key, val in data.items()}
    elif isinstance(data, list):
        return [c.replace(get_index(capture_enum_regex.findall(c), 0, ""), "") for c in data]
    else:
        raise Exception(f"Type of data not supported {type(data)}")


def process_vantage_data(data):
    metadata = data.get("Meta Data", None)
    if metadata:
        try:
            info = clean_enumeration(metadata)
        except Exception as err:
            LOG.ERROR(f"ERROR cleaning info: {metadata}")
            info = metadata
    else:
        info = {}
    data_key = [k for k in data.keys() if k != "Meta Data"][0]      # 'Time Series (Daily)' or 'Time Series FX (Weekly)'
    dat = data[data_key]
    return info, dat


async def save_stock_info(info_file, info, old_info=None, create=True):
    """Save/overwrite info data if previous exists or create is True"""
    into2write = info if old_info is None else {**old_info, **info}     # Select new or merge
    write = True if (info_file.exists() or create) else False
    if write:
        async with aiofiles.open(info_file.as_posix(), mode="w") as f:
            await f.write(json.dumps(into2write, indent=2).encode('ascii', 'ignore').decode('ascii'))


async def read_info_file(info_file, check=True, verbose=VERBOSE):
    if not info_file:
        return {}
    if info_file.exists():
        async with aiofiles.open(info_file, "r") as info:
            data = await info.read()
            if verbose > 1:
                LOG.info(f"Info file read:{get_tabs('', prev=15)}{info_file}")
            return json.loads(data)
    else:
        if check:
            LOG.error(f"ERROR: No info found at {info_file}")
        if verbose > 1:
            LOG.warning(f"Info file: {info_file}\tDO NOT EXISTS!")
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
            LOG.info(f"Updating {symbol} info:{get_tabs(symbol, prev=15)}OK")
    except Exception as err:
        LOG.error(f"ERROR updating info: {info_file}. Msg: {err.__repr__()} {traceback.print_tb(err.__traceback__)}")


def clean_pandas_data(dat):
    data = pd.DataFrame.from_dict(dat, orient="index")
    # Apply clean names to columns and index
    column_names = clean_enumeration(data.columns.tolist())
    data.columns = column_names
    data.index.name = 'date'
    data.sort_index(axis=0, inplace=True, ascending=True)  # Sort by date
    return data


def save_pandas_data(file_name, dat, update=False, verbose=VERBOSE):
    try:
        data = clean_pandas_data(dat)

        if update:
            old_data = read_pandas_data(file_name)
            prior_3, prior_2, prior_1 = old_data.index[-3:]

            days_step = (prior_2 - prior_3).days
            if days_step >= 28:
                # Monthly data
                if (last_day_of_month(prior_1) - prior_1).days > 0:
                    old_data.drop(old_data.tail(1).index, inplace=True)
                next_dt = last_day_of_month(old_data.index[-1] + relativedelta(months=+1))
            elif days_step == 7:
                # Weekly data
                if (end_of_week(prior_1) - prior_1).days > 0:
                    old_data.drop(old_data.tail(1).index, inplace=True)
                next_dt = end_of_week(old_data.index[-1] + relativedelta(days=+6))
            else:
                # Daily data
                next_dt = old_data.index[-1] + relativedelta(days=+1)

            # date_format = datetime_format(idx)
            try:
                idx = data.index.get_loc(next_dt.strftime("%Y-%m-%d"))
                updated_data = pd.concat((old_data, data.iloc[idx:, :]))
            except KeyError:
                # No new data
                updated_data = old_data

            updated_data.reset_index().to_csv(file_name, index=False, compression="infer")  # Update
        else:
            data.reset_index().to_csv(file_name, index=False, compression="infer")          # Save

        if verbose > 1:
            symbol = file_name.parent.name
            LOG.info(f"Saved {symbol} data:{get_tabs(symbol, prev=12)}[{file_name.stem}] OK")
    except Exception as err:
        LOG.error(f"ERROR saving data:\t\t{file_name.parent.name + file_name.stem} "
                  f"{err.__repr__()} {traceback.print_tb(err.__traceback__)}")


def read_pandas_data(file_name):
    if not file_name.exists():
        LOG.error(f"ERROR: data not found for {file_name}")
        return None
    return pd.read_csv(file_name, parse_dates=['date'], index_col='date', date_parser=dateparse)


async def update_stock(symbol, semaphore, category="daily", max_gap=0, api="vantage", verbose=VERBOSE):
    folder_name, file_name = build_path_and_file(symbol, category)
    info_file = build_info_file(folder_name, category)
    info = None

    try:
        if folder_name.exists() and file_name.exists():
            # Verify how much must be updated
            data_stored = read_pandas_data(file_name)
            last_date = data_stored.index[-1]

            if delta_surpassed(last_date, max_gap, category):
                LOG.info(f"Updating {symbol} data...")
                # Retrieve only last range (alpha_vantage 100pts)
                data = await query_data(symbol, semaphore, category=category, api="vantage", outputsize="compact")
                info, dat = process_vantage_data(data)

                save_pandas_data(file_name, dat, update=True, verbose=verbose)
            else:
                LOG.info(f"Updating {symbol}:{get_tabs(symbol, prev=10)}Ignored. Data {category} < {max_gap}d old")
                return
        else:
            # Download and save new data
            LOG.info(f"Updating {symbol} ...")
            data = await query_data(symbol, semaphore, category=category, api=api)
            if data is None:
                return
            info, dat = process_vantage_data(data)
            save_pandas_data(file_name, dat, verbose=verbose)

        # Save/Update info
        if info:
            await update_stock_info(info_file, info)

        LOG.info(f"Updating {symbol}:{get_tabs(symbol, prev=10)}Finished")
    except Exception as err:
        LOG.info(f"Updating {symbol}:{get_tabs(symbol, prev=10)}ERROR: {err.__repr__()} {traceback.print_tb(err.__traceback__)}")


def retrieve_stock_list(symbols, category="daily", gap=7, api="vantage",
                        limit=MAX_CONNECTIONS, verbose=VERBOSE):
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

    if isinstance(api, list):
        tasks = (update_stock(nsymbol, sem, category=category, max_gap=gap, api=napi)
                 for nsymbol, napi in zip(symbols, api))
    else:
        tasks = (update_stock(symbol, sem, category=category, max_gap=gap, api=api) for symbol in symbols)

    loop = asyncio.get_event_loop()
    loop.run_until_complete(asyncio.gather(*tasks))


def search_symbol(symbols=None, api="vantage", limit=MAX_CONNECTIONS, verbose=VERBOSE):
    if symbols == None:
        print('Function Help:\n' \
              '\tProvide a list of symbols, references, possible names, ISIN ref, SEDOL ref...\n' \
              '\t\tex: ["SSE", "GB0007908733"]\n' \
              '\tFor each entry the function returns up to 10 possible matches with a score\n')
        return
    if isinstance(symbols, str):
        symbols = [symbols]

    sem = asyncio.Semaphore(value=limit)

    if isinstance(api, list):
        tasks = (query_data(nsymbol, sem, category="search", api=napi, verbose=verbose)
                 for nsymbol, napi in zip(symbols, api))
    else:
        tasks = (query_data(symbol, sem, category="search", api=api, verbose=verbose) for symbol in symbols)

    loop = asyncio.get_event_loop()
    return loop.run_until_complete(asyncio.gather(*tasks))


def find_data(ref, db):
    idx = ref.parent.name
    data = [entry for entry in db if entry["symbol"] == idx]
    if len(data) > 0:
        return data[0]
    else:
        LOG.warning(f"WARNING: Reference {idx} not found")
        return {}


def update_info_with_search(symbols=None, api="vantage", verbose=VERBOSE):
    if symbols is None:
        # Update existing folders (except currencies)
        stock_folders = [x for x in DATA_FOLDER.iterdir() if x.is_dir() and "_" not in x.name]
        symbols = [x.name for x in stock_folders]

    # Search symbols
    info_from_symbols = search_symbol(symbols, api=api, verbose=verbose)
    info_from_symbols = [data['bestMatches'][0] for data in info_from_symbols]
    info_from_symbols = [clean_enumeration(k) for k in info_from_symbols]

    # Build folders list (and create if they don't exist) and file list
    if "stock_folders" not in locals():
        stock_folders, _ = list(zip(*[build_path_and_file(symbol, "any") for symbol in symbols]))
    info_files = [build_info_file(folder, variation) for folder in stock_folders for variation in INFO_VATIATIONS]

    info_groups = [(info_f, find_data(info_f, info_from_symbols)) for info_f in info_files]
    # Clean empty groups
    info_groups = [grp for grp in info_groups if not grp[1] == {}]

    # Update info
    loop = asyncio.get_event_loop()
    loop.run_until_complete(
        asyncio.gather(*(update_stock_info(file_ref, info,
                                           create=False,
                                           verbose=verbose)
                         for file_ref, info in info_groups))
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
    problematic = ["RDS.A"]

    retrieve_stock_list(symbols, category="daily", gap=7)


if __name__ == "__main__":
    update_info_with_search()
