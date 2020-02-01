import asyncio
import aiohttp

from src.config import HEADERS

# Yahoo! YQL API
PUBLIC_API_URI = 'https://query.yahooapis.com/v1/public/yql'
OAUTH_API_URI = 'https://query.yahooapis.com/v1/yql'
DATATABLES_URI = 'store://datatables.org/alltableswithkeys'


async def yql_query(yql):
    params = {"q": yql, "format": "json", "env": DATATABLES_URI}
    print(f"Getting yql: {yql}")
    async with aiohttp.ClientSession() as session:
        async with session.get(PUBLIC_API_URI, headers=HEADERS, params=params) as resp:
            data = await resp.json()
    return data


# Import libraries
import requests
from bs4 import BeautifulSoup
# import requests_html
import lxml.html as lh
import pandas as pd
import re
from datetime import datetime
from datetime import timedelta
# import mysql.connector as sql
# import DBcm
import time
import unidecode #used to convert accented words
config = {
    "host": "127.0.0.1",
    "user": "root",
    "password": "root",
    "database": "stockdb",
}


if __name__ == "__main__":
    ### Extract from Yahoo Link ###
    ticker_list = ["IDBI.NS", "BHARTIARTL.NS"]
    for ticker in ticker_list:
        url = 'https://in.finance.yahoo.com/quote/' + ticker
        session = requests.Session()
        r = session.get(url)
        content = BeautifulSoup(r.content, 'lxml')
        try:
            price = str(content).split('data-reactid="14"')[4].split('</span>')[0].replace('>', '')
        except IndexError as e:
            price = 0.00
        price = price or "0"
        try:
            price = float(price.replace(',', ''))
        except ValueError as e:
            price = 0.00
        time.sleep(1)
        # with DBcm.UseDatabase(config) as cursor:
        #     _SQL = """insert into tickers
        #               (ticker, price, company_name, listed_exchange, category)
        #               values
        #               (%s, %s, %s, %s, %s)"""
        print(ticker[0], price, ticker[1], ticker[2], ticker[3])

        print(unidecode.unidecode(ticker[0]), price, unidecode.unidecode(ticker[1]), unidecode.unidecode(ticker[2]),
        unidecode.unidecode(ticker[3]))
    print('completed...')
