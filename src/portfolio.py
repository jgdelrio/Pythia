"""
Functionality to manage a Portfolio of stocks
"""
import re
import json
import yaml
import numpy as np
import pandas as pd
from collections import namedtuple

from src.config import *
from src.utils import LOG, NpEncoder, custom_yaml_text
from src.fx_tools import FxManager
from src.api_manager import search_latest_stock_data
from src.overall_commands import get_stocks_table

# TODO: Incorporate dividends in the evaluation


_PORTFOLIO_PROPERTIES = ["Symbol", "ISIN", "Name", "Quantity", "Price", "Factor", "Currency", "Market",
                          "Date", "Fees", "SettledCurrency", "FxExchange", "TotalCost", "Group"]

PortfolioPropertiesTuple = namedtuple("PortfolioProperties", " ".join(_PORTFOLIO_PROPERTIES))


class PortfolioManager:
    def __init__(self, verbose=VERBOSE):
        self.portfolios = {}                    # bucket of portfolios
        self.reports = {}                       # bucket of reports organized by portfolio
        self._performance = None
        self.stocks_info = get_stocks_table()
        self.fx_manager = FxManager()
        self.verbose = verbose
        self.load_existing_portfolios()
        self.store_references()

    def empty_order(self):
        return PortfolioPropertiesTuple(*[None] * len(_PORTFOLIO_PROPERTIES))

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
            elif not all([prop in order.keys() for prop in _PORTFOLIO_PROPERTIES for order in orders]):
                raise ValueError(f"the orders must contain all these properties {_PORTFOLIO_PROPERTIES}")
        else:
            raise TypeError("orders must be a dictionary or a list of dictionaries")

        if portfolio_name in self.portfolios.keys():
            self.portfolios[portfolio_name].extend(orders)
        else:
            self.portfolios[portfolio_name] = orders

    def save_portfolio(self, portfolio_name, mode="yml"):
        if portfolio_name not in self.portfolios.keys():
            raise ValueError(f"Portfolio {portfolio_name} not found")
        portfolio = self.portfolios[portfolio_name]

        if mode == "json":
            portfolio_ref = PORTFOLIOS_FOLDER.joinpath(portfolio_name + ".json")
            self.__load_and_append(portfolio_ref)
            with open(portfolio_ref.as_posix(), "w") as pf:
                pf.write(json.dumps(portfolio, indent=2).encode('ascii', 'ignore').decode('ascii'))
        elif mode in ["yml", "yaml"]:
            portfolio_ref = PORTFOLIOS_FOLDER.joinpath(portfolio_name + ".yml")
            self.__load_and_append(portfolio_ref)
            with open(portfolio_ref.as_posix(), "w+") as pf:
                pf.write(custom_yaml_text(portfolio, _PORTFOLIO_PROPERTIES).encode('ascii', 'ignore').decode('ascii'))

    def save_report(self, portfolio_name, date_ref=None, mode="json"):
        if portfolio_name not in self.reports.keys():
            raise ValueError(f"Portfolio {portfolio_name} not found")
        if date_ref is None:
            date_references = list(self.reports[portfolio_name].keys())
            if len(date_references) > 0:
                date_references.sort()
                date_ref = date_references[-1]
            else:
                LOG.warning(f"There are no reports for Portfolio {portfolio_name}")
        else:
            if date_ref not in self.reports[portfolio_name].keys():
                raise ValueError(f"The date reference {date_ref} was not found in Portfolio {portfolio_name}")

        if mode == "json":
            report_ref = REPORTS_FOLDER.joinpath(portfolio_name + "_" + date_ref + ".json")
            with open(report_ref.as_posix(), "w") as pf:
                pf.write(json.dumps(self.reports[portfolio_name][date_ref],
                                    indent=2, cls=NpEncoder).encode('ascii', 'ignore').decode('ascii'))
        elif mode in ["yml", "yaml"]:
            report_ref = REPORTS_FOLDER.joinpath(portfolio_name + "_" + date_ref + ".yml")
            with open(report_ref.as_posix(), "w") as pf:
                pf.write(yaml.dump(self.reports[portfolio_name][date_ref]).encode('ascii', 'ignore').decode('ascii'))

    def __load_and_append(self, portfolio):
        """
        Verifies if the portfolio exists. If that's the case then loads the existing data and
        append the new data to the pre-existing data
        :param portfolio: portfolio name or reference
        """
        old_portfolio = self.load_portfolio(portfolio, to_variable=True)
        if old_portfolio != -1:
            # Portfolio found. Attach new orders to portfolio
            new_portfolio = self.portfolios[portfolio.stem]

            for new in new_portfolio:
                if new not in old_portfolio:
                    old_portfolio.append(new)
            self.portfolios[portfolio.stem] = old_portfolio

    def load_portfolio(self, portfolio_file, to_variable=False, sort=["Group", "Date"]):
        """
        Load the portfolio referenced into the Portfolio Manager
        :param portfolio_file: name with/without suffix and with/without path
        :param to_variable:    returns the loaded portfolio instead of loading it into the PortfolioManager
        :param sort:           sort the orders by the provided fields
        """
        file_ref = pathlib.Path(portfolio_file)
        portfolio_name = file_ref.stem
        suffix = ".yml" if file_ref.suffix == "" else file_ref.suffix
        portfolio_path = PORTFOLIOS_FOLDER if file_ref.parent == "." else file_ref.parent
        ref = portfolio_path.joinpath(portfolio_name + suffix)

        if ref.exists():
            # Load Portfolio data
            with open(ref.as_posix(), "r") as pf:
                if suffix == ".json":
                    portfolio_data = json.load(pf)
                elif suffix == ".yml":
                    portfolio_data = yaml.load(pf, Loader=yaml.FullLoader)
            # Fill in name with info if available
            for ind, stock in enumerate(portfolio_data):
                if stock["Name"] == "":
                    try:
                        name = [k for k in self.stocks_info[self.stocks_info["Symbol"] == stock["Symbol"]]["Name"]
                                if k not in [None, ""]][0]
                    except IndexError:
                        name = ""
                    portfolio_data[ind]["Name"] = name

            if sort:
                indices_sorted = pd.DataFrame.from_dict(portfolio_data).sort_values(sort).index.tolist()
                portfolio_data = [portfolio_data[x] for x in indices_sorted]

            if to_variable:
                return portfolio_data
            else:
                self.portfolios[portfolio_name] = portfolio_data

        else:
            LOG.warning(f"The portfolio file {portfolio_file} was not found at {ref}")
            return -1

    def load_existing_portfolios(self, suffix=[".json", ".yml"], folder=None):
        folder = PORTFOLIOS_FOLDER if folder is None else pathlib(folder)
        # Detect existing portfolios
        portfolio_refs = [x for x in folder.iterdir() if x.is_file() and x.suffix in suffix]
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
        cost_grp, currencies = zip(*[(order["TotalCost"], order["SettledCurrency"]) for order in self.portfolios[portfolio]])
        return sum(cost_grp)

    def store_references(self):
        full_portfolio = pd.DataFrame()
        for key, val in self.portfolios.items():
            full_portfolio = pd.concat([full_portfolio, pd.DataFrame.from_dict(val)], axis=0)

        full_portfolio = full_portfolio[["Symbol", "ISIN"]]

        db_ref = DATA_FOLDER.joinpath(ISIN_DB)
        if db_ref.exists():
            prev_db = pd.read_csv(db_ref)
        else:
            prev_db = pd.DataFrame()

        db = pd.concat([prev_db, full_portfolio], axis=0)
        db = db.groupby(["Symbol", "ISIN"]).count().reset_index()
        db.to_csv(db_ref, index=False)

    @staticmethod
    def report_from_order(entry_order, exit_order):
        symbol = entry_order["Symbol"]
        days_diff = (datetime.strptime(exit_order["Date"], "%Y-%m-%d") -
                     datetime.strptime(entry_order["Date"], "%Y-%m-%d")).days
        fees = entry_order["Fees"] + exit_order["Fees"] * abs(entry_order["Quantity"] / exit_order["Quantity"])
        market_gain = entry_order["Factor"] * entry_order["Quantity"] * (exit_order["Price"] - entry_order["Price"]) - fees * entry_order["FxExchange"]
        # Consider possible increase/decrease of the currency
        abs_gain = market_gain * entry_order["FxExchange"] / exit_order["FxExchange"]


        perc_abs_gain = abs_gain / entry_order["TotalCost"]
        perc_market_gain = market_gain / (fees/exit_order["FxExchange"] + entry_order["Quantity"] * entry_order["Price"] * entry_order["Factor"])

        yearly_factor = 356 / days_diff
        perc_yearly_gain = perc_abs_gain * yearly_factor
        perc_yearly_market_gain = perc_market_gain * yearly_factor

        return {
            "Symbol": symbol,
            "ISIN": entry_order["ISIN"],
            "Quantity": entry_order["Quantity"],
            "LastValue": exit_order["Price"],
            "Factor": entry_order["Factor"],
            "Currency": entry_order["Currency"],
            'Group': entry_order["Group"],
            "AbsGain": abs_gain,
            "BookCost": entry_order["TotalCost"],
            "Period": days_diff,
            "Fees": fees,
            "PctAbsGain": perc_abs_gain,
            "YearlyPctGain": perc_yearly_gain,
            "AbsMarketGain": market_gain,
            "PctMarketGain": perc_market_gain,
            "YearlyPctMarketGain": perc_yearly_market_gain
        }

    def aggregate_orders(self, orders):
        norders = len(orders)
        if norders == 0:
            return None
        elif norders == 1:
            return orders[0]
        else:
            qty_array = np.array([order["Quantity"] for order in orders])
            qty_weighted = np.divide(qty_array, np.sum(qty_array))
            abs_gain = np.sum(np.array([order["AbsGain"] for order in orders]))
            book_cost = np.sum(np.array([order["BookCost"] for order in orders]))
            total_fees = np.sum(np.array([order["Fees"] for order in orders]))
            weighted_period = np.sum(np.multiply(qty_weighted, np.array([order["Period"] for order in orders])))
            to_yearly_pct = 365 / weighted_period
            abs_market_gain = np.sum(np.array([order["AbsMarketGain"] for order in orders]))
            pct_market_gain = abs_market_gain / (np.sum(np.multiply(
                np.array([order["BookCost"] for order in orders]),
                np.array([order["AbsMarketGain"] / order["AbsGain"] for order in orders]))))

            aggregated = {
                "Symbol": orders[0]["Symbol"],
                "ISIN": orders[0]["ISIN"],
                "Quantity": np.sum(qty_array),
                "LastValue": orders[-1]["LastValue"],
                "Currency": orders[0]["Currency"],
                'Group': orders[0]["Group"],
                "AbsGain": abs_gain,
                "BookCost": book_cost,
                "Period": weighted_period,
                "Fees": total_fees,
                "PctAbsGain": abs_gain / book_cost,
                "YearlyPctGain": to_yearly_pct * abs_gain / book_cost,
                "AbsMarketGain": abs_market_gain,
                "PctMarketGain": pct_market_gain,
                "YearlyPctMarketGain": to_yearly_pct * pct_market_gain
            }
            return aggregated

    def aggregated_sale(self, orders):
        total_qty = orders.Quantity.sum()
        qty_weighted = (orders.Quantity / total_qty).values
        book_cost = orders.TotalCost.sum()
        time_periods = orders.Date.apply(lambda x: (datetime.now() - datetime.strptime(x, "%Y-%m-%d")).days).values
        weighted_period = (qty_weighted * time_periods).sum()
        to_yearly_pct = 365 / weighted_period

        # Value now
        symbol = orders.Symbol.values[0]
        val = search_latest_stock_data(symbol)["close"]
        fx_exchange = self.fx_manager.query_latest(orders.Currency.values[0], orders.SettledCurrency.values[0])
        stock_factor = orders.Factor.values[0]
        market_value = total_qty * val * stock_factor
        fx_rate = 1 if fx_exchange == 1 else fx_exchange * (1 - FX_FEE)
        value_now = market_value * fx_rate
        sale_fees = orders.Fees.max() * total_qty / orders[orders["Fees"] == orders.Fees.max()].Quantity.values[0]
        total_fees = orders.Fees.sum() + sale_fees

        abs_market_gain = market_value - (orders.TotalCost * orders.FxExchange).sum()
        abs_gain = value_now - book_cost
        pct_market_gain = abs_market_gain / (orders.TotalCost * orders.FxExchange).sum()

        aggregated = {
            "Symbol": symbol,
            "ISIN": orders.ISIN.values[0],
            "Quantity": total_qty,
            "LastValue": val,
            "Currency": orders.Currency.values[0],
            'Group': orders.Group.values[0],
            "AbsGain": abs_gain,
            "BookCost": book_cost,
            "Period": weighted_period,
            "Fees": total_fees,
            "PctAbsGain": abs_gain / book_cost,
            "YearlyPctGain": to_yearly_pct * abs_gain / book_cost,
            "AbsMarketGain": abs_market_gain,
            "PctMarketGain": pct_market_gain,
            "YearlyPctMarketGain": to_yearly_pct * pct_market_gain
        }
        return aggregated

    def global_status(self, stock_report, group=None):
        # Apply group filter
        if group is not None:
            group = group.upper()
            stock_report = [stock for stock in stock_report if stock["Group"] == group]

        # Build report
        book_cost_array = np.array([stock["BookCost"] for stock in stock_report])
        total_cost = np.sum(book_cost_array)

        abs_gain_array = np.array([stock["AbsGain"] for stock in stock_report])
        total_abs_gain = np.sum(abs_gain_array)

        abs_value_array = np.add(book_cost_array, abs_gain_array)
        pct_abs_value = np.divide(abs_value_array, np.sum(abs_value_array))

        global_report = {
            "nSymbols": len(stock_report),
            "nGroups": len(set([stock["Group"] for stock in stock_report])),
            "Quantity": np.sum([stock["Quantity"] for stock in stock_report]),
            "AbsGain": total_abs_gain,
            "BookCost": total_cost,
            "Period": np.sum(np.multiply(pct_abs_value, np.array([stock["Period"] for stock in stock_report]))),
            "Fees": np.sum(np.array([stock["Fees"] for stock in stock_report])),
            "PctAbsGain": total_abs_gain / total_cost if total_cost > 0 else total_abs_gain,
            "YearlyPctGain": np.sum(
                np.multiply(pct_abs_value, np.array([stock["YearlyPctGain"] for stock in stock_report]))),
            "AbsMarketGain": np.sum(np.array([stock["AbsMarketGain"] for stock in stock_report])),
            "PctMarketGain": np.sum(
                np.multiply(pct_abs_value, np.array([stock["PctMarketGain"] for stock in stock_report]))),
            "YearlyPctMarketGain": np.sum(
                np.multiply(pct_abs_value, np.array([stock["YearlyPctMarketGain"] for stock in stock_report])))
        }
        return global_report

    def global_by_group(self, stock_report, groups):
        global_by_group = {}
        for group in groups:
            global_by_group[group] = self.global_status(stock_report, group=group)
        return global_by_group

    def portfolio_report(self, portfolio_name, group=None):
        """
        Build report for open and closed positions, by groups and aggregated to global status
        storing the output into the reserved 'reports' field of the PortfolioManager.
        :param portfolio_name: name of the portfolio to evaluate
        :param group:          if the analysis if for one specific group
        """
        if portfolio_name not in self.portfolios.keys():
            raise ValueError(f"Portfolio {portfolio_name} not found. It is not possible to build report")
        # Initialization
        stock_report = []
        stock_report_closed = []
        # Load Portfolio
        orders = pd.DataFrame.from_dict(self.portfolios[portfolio_name])
        # Filter group if provided
        if group is not None:
            group = group.upper()
            orders = orders[orders["Group"] == group]
            if orders.shape[0] < 1:
                LOG.warning(f"The group {group} do not have orders within the Portfolio {portfolio_name}")

        orders_grp = orders.groupby(["Group", "Symbol"], axis=0, as_index=True)["Date"].count()
        for grp in orders_grp.index:
            grp_ind = (orders["Group"] == grp[0]) & \
                      (orders["Symbol"] == grp[1])
            orders_selection = orders[grp_ind].sort_values("Date", ascending=True).reset_index()
            closed_operations = []

            # 1) Use sales to close previous positions if possible
            for index, row in orders_selection.iterrows():
                sale_qty = row["Quantity"]
                if sale_qty < 0:
                    # Close orders as per sale
                    for ind in range(index):
                        qty = orders_selection.loc[ind, "Quantity"]
                        if qty > 0:
                            diff = sale_qty + qty
                            if diff <= 0:
                                # Close order and report its performance
                                closed_operations.append(self.report_from_order(orders_selection.iloc[ind, :], row))
                                orders_selection.loc[ind, "Quantity"] = 0
                                orders_selection.loc[index, "Quantity"] = diff
                                if diff == 0:
                                    break
                                else:
                                    sale_qty = diff
                            else:
                                # Adjust qty
                                entry = orders_selection.iloc[ind, :].copy()
                                entry["Quantity"] = -sale_qty
                                qty_factor = entry["Quantity"] / orders_selection.iloc[ind, :]["Quantity"]
                                entry["TotalCost"] *= qty_factor
                                entry["Fees"] *= qty_factor
                                closed_operations.append(self.report_from_order(entry, orders_selection.loc[index, :]))
                                orders_selection.loc[ind, "Quantity"] = diff
                                orders_selection.loc[index, "Quantity"] = 0
                                break
                    if diff < 0:
                        # It's possible to sell more if there has been dividends as stock
                        extra_sale = orders_selection.loc[index, :].copy()
                        extra_sale["TotalCost"] = diff * extra_sale["Price"]
                        extra_sale["Fees"] = row["Fees"] * diff / row["Quantity"]
                        entry = orders_selection.iloc[0, :].copy()
                        entry["Quantity"] = - diff
                        entry["Price"] = 0.1
                        entry["Fees"] = 0
                        entry["TotalCost"] = 1
                        closed_operations.append(self.report_from_order(entry, extra_sale))

            # 3) Aggregate all closed operations for that stock
            if len(closed_operations) > 0:
                stock_report_closed.append(self.aggregate_orders(closed_operations))

            # 2) All possible sales have been considered. Now evaluate gains up to now
            orders2process = orders_selection[orders_selection["Quantity"] > 0]
            if orders2process.shape[0] > 0:
                stock_report.append(self.aggregated_sale(orders2process))

        date_ref = datetime.now().strftime("%Y-%m-%d")

        report = {
            "portfolio": portfolio_name,
            "date": date_ref,
            "open": stock_report,
            "closed": stock_report_closed,
            "globalOpen": self.global_status(stock_report),
            "globalOpenByGroup": self.global_by_group(stock_report, orders["Group"].unique().tolist()),
            "globalClosed": self.global_status(stock_report_closed),
            "globalClosedByGroup": self.global_by_group(stock_report_closed, orders["Group"].unique().tolist())}

        if portfolio_name in self.reports.keys():
            self.reports[portfolio_name][date_ref] = report
        else:
            self.reports[portfolio_name] = {date_ref: report}


def test_manager():
    # Load manager and existing portfolios
    manager = PortfolioManager()
    # manager.save_portfolio("JDR")
    manager.portfolio_report("JDR")
    manager.save_report("JDR")


if __name__ == "__main__":
    test_manager()
