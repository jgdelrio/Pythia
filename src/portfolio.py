"""
Functionality to manage a Portfolio of stocks
"""
import re
import json
import yaml
import pandas as pd

from src.config import *
from src.utils import LOG
from src.fx_tools import FxManager
from src.api_manager import search_latest_stock_data


__PORTFOLIO_PROPERTIES = ["Symbol", "Quantity", "Price", "Currency", "Market", "Date", "Fees",
                          "SettledCurrency", "FxExchange", "TotalCost", "Group"]

PORTFOLIO_PROPERTIES = [f"{ind+1:02d}.{prop}" for ind, prop in enumerate(__PORTFOLIO_PROPERTIES)]


class PortfolioManager:
    def __init__(self, verbose=VERBOSE):
        self.portfolios = {}                    # bucket of portfolios
        self.reports = {}                       # bucket of reports organized by portfolio
        self._performance = None
        self.verbose = verbose
        self.load_existing_portfolios()
        self.fx_manager = FxManager()

    def empty_order(self):
        return {prop: None for prop in PORTFOLIO_PROPERTIES}

    def add_order(self, orders, portfolio_name):
        """
        Add an order with +qty for buy orders and -qty for sell orders to a specific portfolio
        :param info: dictionary with all relevant parameters
        """
        if isinstance(orders, dict):
            orders = [orders]
        elif isinstance(orders, list):
            if not all([isinstance(k, dict) for k in orders]):
                raise TypeError("All elements within the orders list must be dictionaries")
            elif not all([prop in order.keys() for prop in PORTFOLIO_PROPERTIES for order in orders]):
                raise ValueError(f"the orders must contain all the following properties {PORTFOLIO_PROPERTIES}")
        else:
            raise TypeError("orders must be a dictionary or a list of dictionaries")

        if portfolio_name in self.portfolios.keys():
            self.portfolios[portfolio_name].extend(orders)
        else:
            self.portfolios[portfolio_name] = orders

    def save_portfolio(self, portfolio_name, mode="yml"):
        if portfolio_name in self.portfolios.keys():
            if mode == "json":
                portfolio_ref = DATA_FOLDER.joinpath(portfolio_name + ".json")
                self.__load_and_append(portfolio_ref)
                with open(portfolio_ref.as_posix(), "w") as pf:
                    pf.write(json.dumps(self.portfolios[portfolio_name],
                                        indent=2).encode('ascii', 'ignore').decode('ascii'))
            elif mode in ["yml", "yaml"]:
                portfolio_ref = DATA_FOLDER.joinpath(portfolio_name + ".yml")
                self.__load_and_append(portfolio_ref)
                with open(portfolio_ref.as_posix(), "w+") as pf:
                    pf.write(yaml.dump(self.portfolios[portfolio_name]).encode('ascii', 'ignore').decode('ascii'))

    def __load_and_append(self, portfolio):
        """
        Verifies if the portfolio exists. If that's the case then loads the existing data and
        append the new data to the pre-existing data
        :param portfolio: portfolio name or reference
        """
        old_portfolio = self.load_portfolio(portfolio, to_variable=True)
        if old_portfolio != -1:
            # Portfolio found. Attach new portfolio
            new_portfolio = self.portfolios[portfolio.stem]

            for new in new_portfolio:
                if new not in old_portfolio:
                    old_portfolio.append(new)
            self.portfolios[portfolio.stem] = old_portfolio

    def load_portfolio(self, portfolio_file, to_variable=False):
        """
        Load the portfolio referenced into the Portfolio Manager
        :param portfolio_file: name with/without suffix and with/without path
        """
        file_ref = pathlib.Path(portfolio_file)
        portfolio_name = file_ref.stem
        suffix = ".yml" if file_ref.suffix == "" else file_ref.suffix
        portfolio_path = DATA_FOLDER if file_ref.parent == "." else file_ref.parent
        ref = portfolio_path.joinpath(portfolio_name + suffix)

        if ref.exists():
            with open(ref.as_posix(), "r") as pf:
                if suffix == ".json":
                    portfolio_data = json.load(pf)
                elif suffix == ".yml":
                    portfolio_data = yaml.load(pf, Loader=yaml.FullLoader)
            if to_variable:
                return portfolio_data
            else:
                self.portfolios[portfolio_name] = portfolio_data

        else:
            LOG.warning(f"The portfolio file {portfolio_file} was not found at {ref}")
            return -1

    def load_existing_portfolios(self, stem="portfolio_", folder=None):
        folder = DATA_FOLDER if folder is None else pathlib(folder)
        # Detect existing portfolios
        portfolio_refs = [x for x in folder.iterdir() if x.is_file() and re.findall(stem + r".*", x.stem)]
        # Load portfolios detected
        for portfolio in portfolio_refs:
            self.load_portfolio(portfolio)

    def rename_portfolio(self, old, new):
        self.portfolios[new] = self.portfolios[old]
        del self.portfolios[old]

    def get_portfolio_names(self):
        return self.portfolios.keys().tolist()

    def get_norders_in_portfolio(self, portfolio):
        if portfolio in self.portfolios.keys():
            return len(self.portfolios[portfolio])
        return 0

    def total_portfolio_cost(self, portfolio, fx="GBP"):
        if portfolio not in self.portfolios.keys():
            raise ValueError(f"Portfolio {portfolio} not found")
        cost_grp, currencies = zip(*[(order["10.TotalCost"], order["08.SettledCurrency"]) for order in self.portfolios[portfolio]])
        return sum(cost_grp)

    def get_value_now(self, order):
        qty = order["02.Quantity"]
        val = search_latest_stock_data(order["01.Symbol"])["close"]
        fx_exchange = self.fx_manager.query_latest(order["04.Currency"], order["08.SettledCurrency"])

        stock_now = {
            "02.Quantity": qty,
            "03.Price": val,
            "06.Date": datetime.now().strftime("%Y-%m-%d"),
            "09.FxExchange": fx_exchange,
            "10.TotalCost": qty * val * fx_exchange
        }
        return stock_now

    @staticmethod
    def report_from_order(entry_order, exit_order):
        symbol = entry_order["01.Symbol"]
        days_diff = (datetime.strptime(exit_order["06.Date"], "%Y-%m-%d") -
                     datetime.strptime(entry_order["06.Date"], "%Y-%m-%d")).days
        abs_gain = -exit_order["10.TotalCost"] - entry_order["10.TotalCost"]
        entry_market_amount = entry_order["02.Quantity"] * entry_order["03.Price"]
        market_gain = -exit_order["02.Quantity"] * exit_order["03.Price"] - \
                      entry_market_amount
        perc_abs_gain = (abs_gain / entry_order["10.TotalCost"]) * 100
        perc_market_gain = (market_gain / entry_market_amount) * 100

        yearly_factor = 356 / days_diff
        perc_yearly_gain = perc_abs_gain * yearly_factor
        perc_yearly_market_gain = perc_market_gain * yearly_factor

        return {
            "Symbol": symbol,
            "AbsGain": abs_gain,
            "BookCost": entry_order["10.TotalCost"],
            "Period": days_diff,
            "PctAbsGain": perc_abs_gain,
            "YearlyPctGain": perc_yearly_gain,
            "AbsMarketGain": market_gain,
            "PctMarketGain": perc_market_gain,
            "YearlyPctMarketGain": perc_yearly_market_gain
        }

    def portfolio_report(self, portfolio, group=None):
        if portfolio not in self.portfolios.keys():
            raise ValueError(f"Portfolio {portfolio} not found. It is not possible to build report")

        report_accum = []
        report_closed = []
        orders = pd.DataFrame.from_dict(self.portfolios['portfolio_JDR'])
        orders_grp = orders.groupby(["11.Group", "01.Symbol", "05.Market"], axis=0, as_index=True)["06.Date"].count()
        for grp in orders_grp.index:
            grp_ind = (orders["11.Group"] == grp[0]) & \
                      (orders["01.Symbol"] == grp[1]) & \
                      (orders["05.Market"] == grp[2])
            orders_selection = orders[grp_ind].sort_values("06.Date", ascending=True).reset_index()
            for index, row in orders_selection.iterrows():
                sale_qty = row["02.Quantity"]
                if sale_qty < 0:
                    # Close orders as per sale
                    for ind in range(index):
                        qty = orders_selection["02.Quantity"][ind]
                        if qty > 0:
                            diff = sale_qty + qty
                            if diff <= 0:
                                # Close order and report its performance
                                report_closed.append(self.report_from_order(orders_selection.iloc[ind, :], row))
                                orders_selection["02.Quantity"][ind] = 0
                                if diff == 0:
                                    break
                                else:
                                    sale_qty = diff
                            else:
                                # Adjust qty
                                orders_selection["02.Quantity"][ind] = diff
            # All possible sales have been considered. Now evaluate possible gains until now
            for index, row in orders_selection.iterrows():
                sale_qty = row["02.Quantity"]
                if sale_qty > 0:
                    value_now = get_value_now(row)

                    self.report_from_order(row, value_now)



        report = {
            "accum": report_accum,
            "closed": report_closed }

        if portfolio in self.reports.keys():
            self.reports[portfolio].append(report)
        else:
            self.reports[portfolio] = [report]




def test_manager():
    # Load manager and existing portfolios
    manager = PortfolioManager()
    manager.portfolio_report("portfolio_JDR")


if __name__ == "__main__":
    test_manager()
