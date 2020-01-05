# Pythia: Pythonic library for stocks and portfolio management

## What is it?

Pythia was the name of the high priestess of the Temple of Apollo at Delphi who also served as the oracle, also known as the Oracle of Delphi.
The goal of the package is to provide a building block to retrieve info about stocks, manage portfolios and in the future provide strategies for trading.

## Main Features

Here are just a few of the things incorporated.

- Asynchronous retrieval of stocks (open, high, low, close) and currencies (usd, gdb, bitcoin...)
- Storage of portfolios with one or multiple providers
- Calculation of performance (absolute and yearly performance) for open and closed positions

## Main Dependencies

- aiofiles
- aiohttp
- asyncio
- Cython
- lapack
- numpy
- pandas
- python-dateutil
- pytz
- PyYAML
- zipline

## Quickstart

The first step if using 'alpha vantage' api is to complete the key.yml file at the root of the library with a valid [api key](https://www.alphavantage.co/support/#api-key) and store it as:

```alpha_vantage: "my_key_example"```

The following code downloads the stocks data asynchronously:

```
from src.api_manager import retrieve_stock_list

symbols = ["AMAT", "AMZN", "ATVI", "GOOG", "MMM", "RNSDF", "XOM"]
retrieve_stock_list(symbols, category="daily", gap=2)
```

Get a pandas dataframe of existing stocks info, or currencies:

```
from src.overall_commands import get_stocks_table, get_fx_table

stocks_info = get_stocks_table(verbose=3)
currencies_info = get_fx_table(mode="fx", verbose=3)
crypto_currencies_info = get_fx_table(mode="crypto", verbose=3)
```

To load data from a portfolio, first we need a yml file at the portfolios folder following the structure of the example.
Loading the existing portfolios happens automatically when creating a new portfolio manager object although it can be loaded later as well.

```
from src.portfolio import PortfolioManager

manager = PortfolioManager()
```

We can generate a performance report using the name of the file of the portfolio and save the results to a file if we want:

```
manager.portfolio_report("my_portfolio")
manager.save_report("my_portfolio")
```

## Next Developments

- Add dividends into the performance report
- Retrieve and store fundamentals
- Retrieve and store indices data and data by sector
- Incorporate investment analysis of strategies
