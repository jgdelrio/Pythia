"""
My allies End-points and Parameters

Main page:   https://www.myallies.com/
- News feed
- Company news
- Company details
"""


MY_ALLIES_URI = "https://myallies-breaking-news-v1.p.rapidapi.com"


def get_api_function(category):
    category = category.lower()
    if category in ["top-news", "topnews", "top_news"]:
        function = "GetTopNews"
    elif category == "news":
        # <uri>/news/<symbol>   ex:  "https://myallies-breaking-news-v1.p.rapidapi.com/news/OVTI"
        function = "news"
    elif category in ["last-price", "last-value", "lastprice", "lastvalue"]:
        # "https://myallies-breaking-news-v1.p.rapidapi.com/GetCompanyDetailsBySymbol"    params={"symbol":"GOOG"}
        function = "GetCompanyDetailsBySymbol"
    else:
        raise ValueError(f"invalid category {category}")
    return function


def myallies_query(symbol, category, key=None):
    url = MY_ALLIES_URI
    category = category.lower()

    if category == "news":
        # Retrieval of news
        function = get_api_function(category)
        params = {}


    elif category == "sector":
        function = get_api_function(category)
        params = {"function": function, "apikey": key}

    else:
        raise Exception(f"Category {category} not found")

    # Use extra parameters provided
    params = {**params}
    return url, params