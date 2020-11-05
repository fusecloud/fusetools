"""
Financial tasks and calculations.

|pic1|
    .. |pic1| image:: ../images_source/financial_tools/interactivebrokers1.png
        :width: 50%
"""

import fix_yahoo_finance as yf
import alpha_vantage
import pandas as pd
import numpy as np
from alpha_vantage.timeseries import TimeSeries
import pandas as pd
import numpy as np
from datetime import datetime
from ib_insync import *


class Misc:
    """

    Miscellaneous functions for financial tasks.

    """

    @classmethod
    def round_nickel(cls, x, base=0.05):
        """

        Rounds a decimal to the nearest base provided.

        :param x: Decimal to round.
        :param base: Base to round decimal to.
        :return: Decimal rounded to nearest base provided.
        """
        return round(base * round(x / base), 2)


class Quotes:
    """
    Functions for retrieving stock quotes

    """

    @classmethod
    def alpha_vantage(cls, ticker, api_key, freq, size):
        """
        Pull stock quote data from the Alphavantage API.

        :param ticker: Stock ticker to query for quotes
        :param api_key: Alphavantage API key
        :param freq: Ticker frequency (ex: D for 'Daily' or W for 'Weekly')
        :param size: Number of data points by frequency to pull
        :return: Quote data from Alphavantage API
        """

        ts = TimeSeries(key=api_key, output_format='pandas')
        if freq == "D":
            data, meta_data = ts.get_daily_adjusted(symbol=ticker, outputsize=size)
        elif freq == "W":
            data, meta_data = ts.get_weekly_adjusted(symbol=ticker, outputsize=size)
        elif freq == "M":
            data, meta_data = ts.get_monthly_adjusted(symbol=ticker, outputsize=size)

        return data, meta_data

    @classmethod
    def yahoo_finance_quotes(cls, ticker, from_date, to_date, time_frame='d'):
        """
        Pull stock quote data from the YahooFinance API.

        :param ticker: Stock ticker to query for quotes
        :param from_date: Date to pull quotes from
        :param to_date: Date to pull quotes until
        :param time_frame: Ticker frequency (ex: D for 'Daily' or W for 'Weekly')
        :return: Quote data from YahooFinance API
        """

        data = yf.download(ticker, from_date, to_date)
        data.reset_index(inplace=True)

        d = {"Date": 'datetime2', 'Open': 'o', 'High': 'h', "Low": "l", "Close": 'c', "Adj Close": "ac", "Volume": "v"}
        data.columns = data.columns.map(lambda col: d[col])
        #     data['datetime2'] = data['datetime2'].astype(datetime)
        data['datetime2'] = pd.to_datetime(data['datetime2'])
        data['ticker'] = ticker

        if time_frame == 'd':
            return data

        elif time_frame == 'w':
            data['week_number'] = \
                data['datetime2'].dt.year.astype(str) + \
                data['datetime2'].dt.week.astype(str).str.zfill(2)

            data = (data
                .groupby("week_number")
                .agg(
                ticker=("ticker", "first"),
                date_start=("datetime2", np.min),
                date_end=("datetime2", np.max),
                o=("o", "first"),
                c=("c", "last"),
                h=("h", np.max),
                l=("l", np.min),
                v=("v", np.sum)
            )
            ).reset_index(drop=True)

            return data


class InteractiveBrokers:
    """
    Interactive Brokers API.

    .. image:: ../images_source/financial_tools/interactivebrokers1.png
    """

    @classmethod
    def ib_connect(cls, port, client_id, host='127.0.0.1'):
        """

        Connects IB API to an Interactive Brokers TWS application.

        :param port: TWS port to allow API connection.
        :param client_id: TWS client id.
        :param host: TWS host.
        :return: IB connection object for account.
        """
        ib = IB()
        try:
            ib.connect(host, port, clientId=client_id)
        except:
            ib.disconnect()
            ib.connect(host, port, clientId=client_id)
        return ib

    @classmethod
    def ib_place_order(cls,
                       ib,
                       ticker,
                       action,
                       trade_type,
                       buy_d=False,
                       sell_d=False,
                       profit_taker_d=False
                       ):
        """

        Places an order.

        :param ticker: Ticker to execute trade on.
        :param action: 'BUY' or 'SELL'
        :param trade_type: 'bracket', 'limit', 'market' order.
        :param buy_d: Dictionary in form of {'size':shares_to_order,'price':price_to_execute}
        :param sell_d: Dictionary in form of {'size':shares_to_order,'price':price_to_execute}
        :param profit_taker_d: Dictionary of {size:price} for upside limit prices.
        :return: JSON response of trades placed.
        """

        # create contract object on ticker
        contract = Stock(ticker, 'SMART', 'USD')
        ib.qualifyContracts(contract)

        if trade_type == "bracket":
            bracketOrder = ib.bracketOrder(
                action=action,
                quantity=buy_d.get("size") if action == "BUY" else sell_d.get('size'),
                limitPrice=buy_d.get("price") if action == "BUY" else sell_d.get('price'),
                takeProfitPrice=profit_taker_d.get("price"),
                stopLossPrice=sell_d.get("price")
            )

            trades = []
            for o in bracketOrder:
                trade = ib.placeOrder(contract, o)
                trades.append(trade)

        elif trade_type == "market":
            order = MarketOrder(
                action=action,
                totalQuantity=buy_d.get("size") if action == "BUY" else sell_d.get("size")
            )

            trades = ib.placeOrder(contract=contract, order=order)

        elif trade_type == "limit":
            order = LimitOrder(
                action=action,
                totalQuantity=buy_d.get("size") if action == "BUY" else sell_d.get("size"),
                lmtPrice=buy_d.get("price") if action == "BUY" else sell_d.get("price"),
                tif='GTC'  # https://github.com/erdewit/ib_insync/issues/64
            )

            trades = ib.placeOrder(contract=contract, order=order)

        return trades

    @classmethod
    def ib_get_open_orders(cls, ib):
        """
        Returns a Pandas DataFrame of details for open orders in the account.

        :param ib: InteractiveBrokers account instance
        :return: Pandas DataFrame of open orders
        """

        open_trades = [x for x in ib.openTrades()]
        if len(open_trades) == 0:
            print("No open trades...")
            # make dataframe
            oo_df = pd.DataFrame({
                "oo_ids": [],
                "oo_sizes": [],
                "oo_tickers": [],
                "oo_types": [],
                "oo_prices": [],
                "oo_actions": [],
                "oo_orders": [],
                "oo_contracts": []
            })
        else:
            oo_orders = [x.order for x in open_trades]
            oo_contracts = [x.contract for x in open_trades]
            oo_ids = [x.order.orderId for x in open_trades]
            oo_actions = [x.order.action for x in open_trades]
            oo_sizes = [x.order.totalQuantity for x in open_trades]
            oo_types = [x.order.orderType for x in open_trades]
            oo_prices = [x.order.lmtPrice for x in open_trades]
            oo_tickers = [x.contract.symbol for x in open_trades]

            # make dataframe
            oo_df = pd.DataFrame({
                "oo_ids": oo_ids,
                "oo_sizes": oo_sizes,
                "oo_tickers": oo_tickers,
                "oo_types": oo_types,
                "oo_prices": oo_prices,
                "oo_actions": oo_actions,
                "oo_orders": oo_orders,
                "oo_contracts": oo_contracts
            })
        return oo_df

    @classmethod
    def ib_get_portfolio(cls, ib):
        """
        Returns a Pandas DataFrame of details for portfolio holdings.

        :param ib: InteractiveBrokers account instance
        :return: Pandas DataFrame of portfolio holding sizes, tickers and cost-bases
        """
        port_sizes = [x.position for x in ib.portfolio()]
        port_tickers = [x.contract.symbol for x in ib.portfolio()]
        port_costs = [x.averageCost for x in ib.portfolio()]
        # port_costs_per_share = [x / y for (x, y) in zip(port_costs, port_sizes)]

        port_df = \
            pd.DataFrame({
                "port_sizes": port_sizes,
                "port_tickers": port_tickers,
                "port_costs": port_costs,
            })
        print(f'''{len(port_df)} open positions...''')
        return port_df

    @classmethod
    def get_stock_price_ss(cls, ib, ticker):
        """
        Returns the latest stock price for a given ticker.

        :param ib: InteractiveBrokers account instance
        :param ticker: Ticker to search
        :return: Latest stock price for a given ticker
        """
        contract = Stock(ticker, 'SMART', 'USD')
        ret = ib.reqMktData(contract, snapshot=True)
        while str(ret.last) == "nan":
            ib.sleep(5)

        return ret.last
