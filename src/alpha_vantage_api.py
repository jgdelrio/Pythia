
ALPHA_VANTAGE_URI = "https://www.alphavantage.co/query?function="


def query_time_series(symbol, outputsize="full", key=None):
    if outputsize not in ["compact", "full"]:
        raise ValueError("invalid value in outputsize parameter")
    if key is None:
        raise ValueError("Please provide a valid key")
    return f"{ALPHA_VANTAGE_URI}TIME_SERIES_DAILY&symbol={symbol}&outputsize={outputsize}&apikey={key}"

