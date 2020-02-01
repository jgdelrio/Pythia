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


### Comments about ML Trading

#### Machine learning Hype

Despite seen Machine Learning everywhere these days and being a "buzz word", ML is very tough to apply in trading.
It's more useful as filtering mechanism rather than a decision tool. 
This is because many times the trading test will be awesome and they will fail in real trading as they overfit.
You may think that cross validation will solve everything but it's really easy to add bias and leak data...

- Average and evaluate on different assets, time frames or periods.
- Use non-conventional splits for train/test sets.
- Add random noise
- Evaluate generalization power
- Monte-Carlo simulation to evaluate multiple scenarios 

#### Risk Evaluation

Pricing models to estimate an asset value in a predefined time horizon include:

- Monte Carlo
- Binomial Trees
- Black-Scholes-Merton

Implied Volatility (IV) typically overstates the fear in the marketplace, but sometimes it does not....

#### Common Sense

KISS (Keep It Simple Stupid) sounds good but it's very easy to get lost on complex and cutting edge techniques and forget the basics...

Simple assumptions, simple statistics and Monte Carlo simulations go a long way.

Risk assessment and position sizing are key to success.

Commissions can rip you off if not taken into account.

Evaluate liquidity (avoid low liquidity assets) and fundamentals.

Track Market makers to understand the market since they tend to control the market inefficiencies.

Focus on consistency, probabilities and risk rather than balance, market value and money.

Evaluate market volatility and other fear/greed indices of the market. When the volatility is high is easier to find buyers if you want to sell, and periods of low volatility tend to finish with an explosion of volatility.




