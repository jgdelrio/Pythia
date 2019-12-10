"""
Alpah Vantage End-points and Parameters
"""

ALPHA_VANTAGE_URI = "https://www.alphavantage.co/query"


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
    else:
        raise ValueError(f"invalid category {category}")
    return function


def alpha_vantage_query(symbol, category, output_size=None, datatype=None, key=None, **kwargs):
    datatype = "json" if datatype is None else datatype  # Valid: csv, json

    if category in ["daily", "daily_adjusted", "weekly", "weekly_adjusted", "monthly", "monthly_adjusted"]:
        # Retrieval of daily time series
        function = get_alpha_vantage_function(category)
        output_size = "full" if output_size is None else output_size    # Valid: full, compact (only 100 points)

        url = ALPHA_VANTAGE_URI
        params = {"function": function, "symbol": symbol, "outputsize": output_size,
                  "datatype": datatype, "apikey": key, **kwargs}

    elif category == "search":
        # Retrieval of best-matching symbols (with scores) and market info
        url = ALPHA_VANTAGE_URI
        function = "SYMBOL_SEARCH"
        params = {"function": function, "keywords": symbol, "datatype": datatype, "apikey": key}
        # f"{ALPHA_VANTAGE_URI}?function=TIME_SERIES_DAILY&symbol={symbol}&outputsize={outputsize}&apikey={key}"

    elif category in ["fx", "fx_rate"]:
        # Retrieval of currency exchange rates
        url = ALPHA_VANTAGE_URI
        function = "CURRENCY_EXCHANGE_RATE"
        if not all([k in kwargs.keys() for k in ["from_currency", "to_currency"]]):
            raise KeyError("Required parameters (from_currency, to_currency) not provided")
        params = {"function": function, "from_currency": kwargs["from_currency"],
                  "to_currency": kwargs["to_currency"], "apikey": key}

    return url, params
