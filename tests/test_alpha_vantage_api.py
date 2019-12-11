import pytest
from hamcrest import *
from src.alpha_vantage_api import *
from src.utils import DelayedAssert


EXPECTED_URL_API = "https://www.alphavantage.co/query"
GET_ALPHA_VANTAGE_FUNCTION_DATA = (["weekly", "TIME_SERIES_WEEKLY"],
                                   ["fx", "FX_DAILY"],
                                   ["fx_daily", "FX_DAILY"],
                                   ["fx_monthly", "FX_MONTHLY"])


@pytest.mark.parametrize("data_in, expected", GET_ALPHA_VANTAGE_FUNCTION_DATA)
def test_get_alpha_vantage_function(data_in, expected):
    output = get_alpha_vantage_function(data_in)
    assert_that(output, equal_to(expected), f"Alpha_vantage_function error")


ALPHA_VANTAGE_QUERY_DATA = (
    ["MMM", "daily_adjusted", None, None, EXPECTED_URL_API,
     {'function': 'TIME_SERIES_DAILY_ADJUSTED', 'symbol': 'MMM', 'outputsize': 'full',
      'datatype': 'json', 'apikey': 'demo_key'}],
    [["EUR", "GBP"], "fx_exchange", "compact", "csv", EXPECTED_URL_API,
     {'function': 'CURRENCY_EXCHANGE_RATE', 'from_currency': 'EUR', 'to_currency': 'GBP', 'apikey': 'demo_key'}],
    [["EUR", "CNY"], "fx_daily", "compact", "json", EXPECTED_URL_API,
     {'function': 'FX_DAILY', 'from_symbol': 'EUR', 'to_symbol': 'CNY', 'outputsize': 'compact',
      'datatype': 'json', 'apikey': 'demo_key'}],
)


@pytest.mark.parametrize("symbol, category, output_size, datatype, expected_url, expected_params", ALPHA_VANTAGE_QUERY_DATA)
def test_alpha_vantage_query(symbol, category, output_size, datatype, expected_url, expected_params):
    output_url, output_params = alpha_vantage_query(symbol, category,
                                                    output_size=output_size, datatype=datatype, key="demo_key")
    delay_assert = DelayedAssert()
    delay_assert.expect(output_url == expected_url,
                        f"Error: {output_url} differs from expected {expected_url}")
    delay_assert.expect(output_params == expected_params,
                        f"Error: {output_params} differs from expected {expected_params}")
    delay_assert.assert_expectations()


if __name__ == "__main__":
    test_alpha_vantage_query(*ALPHA_VANTAGE_QUERY_DATA[2])