from data.updates import FX_UPDATES
from src.api_manager import retrieve_stock_list


def fx_updates():
    retrieve_stock_list(FX_UPDATES, category="fx_daily", gap=7)
    # retrieve_stock_list(FX_UPDATES, category="fx_monthly", gap=7)


if __name__ == "__main__":
    fx_updates()
