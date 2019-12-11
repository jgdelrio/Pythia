"""
Alpah Vantage End-points and Parameters
"""

import re

ALPHA_VANTAGE_URI = "https://www.alphavantage.co/query"
fx_regex = re.compile("^fx_")

def get_alpha_vantage_function(category):
    if category == "daily":
        function = "TIME_SERIES_DAILY"
    elif category == "daily_adjusted":
        function = "TIME_SERIES_DAILY_ADJUSTED"
    elif category == "weekly":
        function = "TIME_SERIES_WEEKLY"
    elif category == "weekly_adjusted":
        function = "TIME_SERIES_WEEKLY_ADJUSTED"
    elif category == "monthly":
        function = "TIME_SERIES_MONTHLY"
    elif category == "monthly_adjusted":
        function = "TIME_SERIES_MONTHLY_ADJUSTED"
    elif category in ["fx", "fx_daily"]:
        function = "FX_DAILY"
    elif fx_regex.match(category):
        function = category.upper()
    else:
        raise ValueError(f"invalid category {category}")
    return function


def alpha_vantage_query(symbol, category, output_size=None, datatype=None, key=None, **kwargs):
    url = ALPHA_VANTAGE_URI
    datatype = "json" if datatype is None else datatype             # Valid: csv, json
    output_size = "full" if output_size is None else output_size    # Valid: full, compact (only 100 points)

    if category in ["daily", "daily_adjusted", "weekly", "weekly_adjusted", "monthly", "monthly_adjusted"]:
        # Retrieval of daily time series
        function = get_alpha_vantage_function(category)

        params = {"function": function, "symbol": symbol, "outputsize": output_size,
                  "datatype": datatype, "apikey": key, **kwargs}

    elif category == "search":
        # Retrieval of best-matching symbols (with scores) and market info
        function = "SYMBOL_SEARCH"
        params = {"function": function, "keywords": symbol, "datatype": datatype, "apikey": key}
        # f"{ALPHA_VANTAGE_URI}?function=TIME_SERIES_DAILY&symbol={symbol}&outputsize={outputsize}&apikey={key}"

    elif category in ["fx_exchange", "fx_rate"] or bool(fx_regex.match(category)):
        if isinstance(symbol, list):
            if len(symbol) == 2:
                from_crrn, to_crrn = symbol
        elif isinstance(symbol, dict):
            if not all([k in symbol.keys() for k in ["from_currency", "to_currency"]]):
                raise KeyError("Required parameters (from_currency, to_currency) not provided")
            from_crrn, to_crrn = symbol["from_currency"], symbol["to_currency"]

        if category in ["fx_exchange", "fx_rate"]:
            # Retrieval of currency exchange rates
            function = "CURRENCY_EXCHANGE_RATE"
            params = {"function": function, "from_currency": from_crrn, "to_currency": to_crrn, "apikey": key}
        else:
            # Retrieval of historical rates
            function = get_alpha_vantage_function(category)
            params = {"function": function, "from_symbol": from_crrn, "to_symbol": to_crrn,
                      "outputsize": output_size, "datatype": datatype, "apikey": key}

    return url, params
