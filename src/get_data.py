import re
import json
import aiohttp
import asyncio
import aiofiles
import pandas as pd
import traceback
from datetime import datetime
from src.alpha_vantage_api import query_time_series
from src.utils import LOG
from src.config import DATA_FOLDER, DEFAULT_STOCK_FILE, DEFAULT_INFO_FILE, MAX_CONNECTIONS, MIN_SEM_WAIT, HEADERS, VERBOSE
from src.keys import alpha_key


clean_names_regex = re.compile("[\w]*$")
dateparse = lambda dates: pd.datetime.strptime(dates, '%Y-%m-%d')


async def query_data(symbol, semaphore, api="vantage", verbose=VERBOSE):
    await semaphore.acquire()
    if verbose > 2:
        LOG.info("Successfully acquired the semaphore")

    if api == "vantage":
        url = query_time_series(symbol, key=alpha_key)
        LOG.info(f"Retrieving {symbol} from '{api}'")

    async with aiohttp.ClientSession() as session:
        async with session.get(url, headers=HEADERS) as resp:
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
    info = {
        "symbol": data["Meta Data"]["2. Symbol"],
        "lastUpdate": data["Meta Data"]["3. Last Refreshed"],
        "timeZone": data["Meta Data"]["5. Time Zone"]
    }
    dat = data["Time Series (Daily)"]
    return info, dat


async def save_stock_info(symbol, info, old_info=None, filename=DEFAULT_INFO_FILE):
    folder_of_symbol = DATA_FOLDER.joinpath(symbol)
    folder_of_symbol.mkdir(parents=True, exist_ok=True)  # Create if doesn't exist

    if old_info is None:
        into2write = info
    else:
        # Merge info
        into2write = {**old_info, **info}

    async with aiofiles.open(folder_of_symbol.joinpath(filename), "w+") as info_file:
        json.dump(into2write, info_file)


async def read_stock_info(symbol, check=True):
    folder_of_symbol = DATA_FOLDER.joinpath(symbol)
    info_file = folder_of_symbol.joinpath(DEFAULT_INFO_FILE)

    if check:
        if not folder_of_symbol.exits():
            raise ValueError(f"ERROR: No folder found for stock {symbol}")
        if not info_file.exists():
            raise ValueError(f"ERROR: No info found for stock {symbol}")

    if info_file.exists():
        async with aiofiles.open(info_file, "r") as f:
            return json.load(f)
    else:
        return {}


def save_stock_data(symbol, data, filename=DEFAULT_STOCK_FILE):
    folder_of_symbol = DATA_FOLDER.joinpath(symbol)
    folder_of_symbol.mkdir(parents=True, exist_ok=True)     # Create if doesn't exist

    stock_data = pd.DataFrame.from_dict(data, orient="index")
    # Apply clean names to columns and index
    column_names = [clean_names_regex.findall(c)[0] for c in stock_data.columns.tolist()]
    stock_data.columns = column_names
    stock_data.index.name = 'date'
    # Sort by date
    stock_data.sort_index(axis=0, inplace=True, ascending=True)
    # Save
    stock_data.reset_index().to_csv(folder_of_symbol.joinpath(filename),
                                    index=False, compression="infer")


def read_stock_file(symbol, filename=DEFAULT_STOCK_FILE):
    folder_of_symbol = DATA_FOLDER.joinpath(symbol)
    if not folder_of_symbol.exists():
        LOG.error(f"ERROR: stock data not found for {symbol}")
        return None

    stock_file = folder_of_symbol.joinpath(filename)
    if stock_file.exists():
        return pd.read_csv(folder_of_symbol.joinpath(filename),
                               parse_dates=['date'],
                               index_col='date',
                               date_parser=dateparse)
    else:
        return None


async def update_stock(symbol, semaphore, max_gap=0):
    folder_of_symbol = DATA_FOLDER.joinpath(symbol)
    try:
        if folder_of_symbol.exists() and DATA_FOLDER.joinpath(symbol).joinpath(DEFAULT_STOCK_FILE).exists():
            # Verify how much must be updated
            data_stored = read_stock_file(symbol)
            delta = (datetime.now() - data_stored.index[-1]).days

            if delta > max_gap:
                # Retrieve 'delta' days
                # TODO: retrieve only specific date range
                data = await query_data(symbol, semaphore, api="vantage")
                info, dat = process_vantage_data(data)

                old_info = await read_stock_info(symbol, check=False)

                LOG.info(f"Updating {symbol} stock data")
                await save_stock_info(symbol, info, old_info=old_info)
                save_stock_data(symbol, dat)
            else:
                LOG.info(f"Ignoring {symbol}. Data available is < {max_gap} days old")

        else:
            # Download and save new data
            data = await query_data(symbol, semaphore, api="vantage")
            if data is None:
                return
            info, dat = process_vantage_data(data)

            await save_stock_info(symbol, info)
            save_stock_data(symbol, dat)

        LOG.info(f"Successfully updated {symbol}")
    except Exception as err:
        LOG.error(f"ERROR while processing {symbol}. Msg: {err.__repr__()} {traceback.print_tb(err.__traceback__)}")


def retrieve_stock_list(symbols, gap=7, limit=MAX_CONNECTIONS):
    sem = asyncio.Semaphore(value=limit)
    loop = asyncio.get_event_loop()
    loop.run_until_complete(
        asyncio.gather(
            *(update_stock(symbol, sem, max_gap=gap) for symbol in symbols)
        )
    )


if __name__ == "__main__":
    symbols = ["AMZN", "ATVI", "GOOG", "MMM", "RNSDF", "XOM"]
    problematic = ["RDS.A"]
    retrieve_stock_list(symbols, gap=7)
