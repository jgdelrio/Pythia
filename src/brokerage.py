from src.fx_tools import FxManager
# TODO: Everything...

fx_manager = FxManager()


rates = {
    "ING": {
        "currency": "EUR",
        "brokerage": 13,
        "fix_rate": [
            ("SP", [(30000, (8, 0)),
                    (1e9, (0, 0.002))]),
            ("UK", [(30000, ((20 + 1) * fx_manager.query_latest("GBP", "EUR")), 0),
                    (1e9, (0, 0.002))]),
            ("EU", [(30000, (20, 0)),
                    (1e9, (0, 0.002))]),
            ("US", [(30000, (20 * fx_manager.query_latest("USD", "EUR"), 0)),
                    (1e9, (0, 0.002))]),
        ],
        "var_rate": [
            ("SP", [(300, (1.1, 0)),
                    (3000, (2.45, 0.00024)),
                    (35000, (4.65, 0.00012)),
                    (70000, (6.4, 0.00007)),
                    (140000, (9.2, 0.00003)),
                    (1e9, (13.4, 0))]),
            ("UK", 0.005),
            ("FR", 0.03),
            ("US", 0.0000174),
        ],
    }
}


def calculate_fees(value, market, provider):
    if provider == "ING":
        total_fee = rates[provider]["brokerage"]


if __name__ == "__main__":
    print(rates)
