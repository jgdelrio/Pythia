"""
Get a Quandl key by registering an account at: https://docs.quandl.com/
Sign up page:    https://www.quandl.com/sign-up-modal?defaultModal=showSignUp&intendedUrl=%2Faccount%2Fprofile

The time-series API gives you a choice of three formats (JSON/XML/CSV) and libraries for multiple tools.

Call example:
    GET https://www.quandl.com/api/v3/datasets/{database_code}/{dataset_code}/data.{return_format}
    curl "https://www.quandl.com/api/v3/datasets/WIKI/FB/data.json?api_key=YOURAPIKEY"

With more options:
    curl "https://www.quandl.com/api/v3/datasets/WIKI/FB.json?column_index=4&start_date=2014-01-01&end_date=2014-12-31&collapse=monthly&transform=rdiff&api_key=YOURAPIKEY"
"""


from src.utils import validate_type
from zipline.api import order_target, record, symbol


QUANTL_URI = "https://www.quandl.com/api/v3/datasets/"


def quandl_query(symbol, category, output_size=None, datatype=None, key=None, **kwargs):
    url = QUANTL_URI
    datatype = "json" if datatype is None else datatype             # Valid: csv, json
    output_size = "full" if output_size is None else output_size    # Valid: full, compact (only 100 points)
    category = category.lower()

    if category in ["daily", "daily-adjusted", "weekly", "weekly-adjusted", "monthly", "monthly-adjusted"]:
        validate_type(symbol, str, "stock_symbol")
        # Retrieval of daily time series
        function = get_api_function(category)

        params = {"function": function, "symbol": symbol, "outputsize": output_size,
                  "datatype": datatype, "apikey": key}

    elif category == "search":
        # Retrieval of best-matching symbols (with scores) and market info
        function = "SYMBOL_SEARCH"
        params = {"function": function, "keywords": symbol, "datatype": datatype, "apikey": key}
        # f"{ALPHA_VANTAGE_URI}?function=TIME_SERIES_DAILY&symbol={symbol}&outputsize={outputsize}&apikey={key}"

    elif category in ["fx_exchange", "fx_rate"] or bool(fx_regex.match(category)):
        # FX exchange requires two values, from what symbol and to what symbol
        if isinstance(symbol, dict):        # Process dict
            if not all([k in symbol.keys() for k in ["from_currency", "to_currency"]]):
                raise KeyError("Required parameters (from_currency, to_currency) not provided")
            from_crrn, to_crrn = symbol["from_currency"], symbol["to_currency"]

        elif isinstance(symbol, str):
            if fx_data_regex.match(symbol):
                from_crrn, to_crrn = symbol.split("_")
            else:
                raise ValueError("Not valid values found")

        elif isinstance(symbol, (list, tuple)):                               # Process list
            validate_currency_pair(symbol)
            from_crrn, to_crrn = symbol

        else:
            raise TypeError("Unexpected type in symbol")

        if category in ["fx_exchange", "fx_rate"]:
            # Retrieval of currency exchange rates
            function = "CURRENCY_EXCHANGE_RATE"
            params = {"function": function, "from_currency": from_crrn, "to_currency": to_crrn, "apikey": key}
        else:
            # Retrieval of historical rates
            function = get_api_function(category)
            params = {"function": function, "from_symbol": from_crrn, "to_symbol": to_crrn,
                      "outputsize": output_size, "datatype": datatype, "apikey": key}

    elif category in ["digital", "digital_fx"] or bool(digital_regex.match(category)):
        # Digital requires two values, the symbol and the market
        if isinstance(symbol, dict):        # Process dict
            if not all([k in symbol.keys() for k in ["symbol", "market"]]):
                raise KeyError("Required parameters (symbol, market) not provided")
            symbol, market = symbol["symbol"], symbol["market"]

        elif isinstance(symbol, str):
            if fx_crypto_regex.match(symbol):
                symbol, market = symbol.split("_")
            else:
                raise ValueError("Not valid values found")
        elif isinstance(symbol, (list, tuple)):                               # Process list
            validate_currency_pair(symbol)
            symbol, market = symbol

        else:
            raise TypeError("Unexpected type in symbol")

        if symbol not in crypto_currencies:
            raise ValueError("The symbol must be a valid cryptocurrency. Please introduce the value in the table")

        if category in ["digital_exchange"]:
            # Retrieval of currency exchange rates
            function = "CURRENCY_EXCHANGE_RATE"
            params = {"function": function, "symbol": symbol, "market": market, "apikey": key}
        else:
            # Retrieval of historical rates
            function = get_api_function(category)
            params = {"function": function, "symbol": symbol, "market": market,
                      "outputsize": output_size, "datatype": datatype, "apikey": key}
    elif category == "sector":
        function = get_api_function(category)
        params = {"function": function, "apikey": key}

    else:
        raise Exception("Category {} not found".format(category))

    # Use extra parameters provided
    params = {**params, **kwargs}
    return url, params


if __name__ == "__main__":
    symbol('AAPL')
    pass
