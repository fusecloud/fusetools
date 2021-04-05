"""
Financial tasks and calculations.

|pic1| |pic2|
    .. |pic1| image:: ../images_source/financial_tools/interactivebrokers1.png
        :width: 50%
    .. |pic2| image:: ../images_source/financial_tools/tos.png
        :width: 50%
"""

import requests
import fix_yahoo_finance as yf
from yahoo_finance_async import OHLC, Interval, History
from alpha_vantage.timeseries import TimeSeries
import pandas as pd
import numpy as np
import json

try:
    from ib_insync import *
except:
    pass
from tda import auth, client


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


class ThinkOrSwim:
    """
    ThinkOrSwim API.

    .. image:: ../images_source/financial_tools/tos.png
    """

    @classmethod
    def authenticate(cls, token_path, api_key, chromedriver_path, redirect_uri='http://localhost:8888/callback'):
        """
        Creates an authenticated ThinkOrSwim API session object.

        :param token_path: Path to API token on disk (if exists or where to save).
        :param api_key: ThinkOrSwim API key.
        :param chromedriver_path: Path to chromedriver executable on disk.
        :param redirect_uri: Redirect url for web authentication.
        :return: Authenticated ThinkOrSwim API session object.
        """
        try:
            c = auth.client_from_token_file(token_path, api_key)
        except FileNotFoundError:
            from selenium import webdriver

            with webdriver.Chrome(chromedriver_path) as driver:
                c = auth.client_from_login_flow(
                    driver, api_key, redirect_uri, token_path)
        return c

    @classmethod
    def pull_quote_history(cls, authentication_object, ticker, frequency_type="daily", start_date=None, end_date=None):
        r = authentication_object.get_price_history(
            ticker,
            period_type=client.Client.PriceHistory.PeriodType.DAY if frequency_type == "5min" else
            client.Client.PriceHistory.PeriodType.YEAR,
            period=client.Client.PriceHistory.Period.ONE_YEAR if frequency_type == "5min" else
            client.Client.PriceHistory.Period.TWENTY_YEARS
            ,
            frequency_type= \
                client.Client.PriceHistory.FrequencyType.MINUTE if frequency_type == "5min" else
                client.Client.PriceHistory.FrequencyType.DAILY if frequency_type == "daily" else
                client.Client.PriceHistory.FrequencyType.WEEKLY if frequency_type == "weekly" else
                client.Client.PriceHistory.FrequencyType.MONTHLY
            ,
            frequency= \
                client.Client.PriceHistory.Frequency.EVERY_THIRTY_MINUTES if frequency_type == "5min" else
                client.Client.PriceHistory.Frequency.DAILY if frequency_type == "daily" else
                client.Client.PriceHistory.Frequency.WEEKLY if frequency_type == "weekly" else
                client.Client.PriceHistory.Frequency.MONTHLY
            ,
            start_datetime=start_date,
            end_datetime=end_date
        )
        """
        Pulls price history for a stock ticker.

        :param authentication_object: Authenticated ThinkOrSwim API session object.
        :param ticker: Stock ticker to pull quotes for.
        :param start_date: Start date of data to pull quotes for (optional).
        :param end_date: End date of data to pull quotes for (optional).
        :return: Stock quotes JSON response object.
        """
        # https://developer.tdameritrade.com/price-history/apis/get/marketdata/%7Bsymbol%7D/pricehistory
        return r

    @classmethod
    def pull_account_watchlists(cls, authentication_object, account_id):
        r = authentication_object.get_watchlists_for_single_account(
            account_id=account_id
        )

        watchlists_json = json.loads(r.content)
        df_list = []
        for idx, row in enumerate(watchlists_json):
            watchlist_names = [x.get("instrument").get("symbol") for x in row.get("watchlistItems")]
            df = \
                pd.DataFrame({
                    "ticker": list(set(watchlist_names)),
                    "name": [row.get("name")] * len(list(set(watchlist_names)))
                })
            df_list.append(df)

        df_all = pd.concat(df_list).reset_index(drop=True)
        df_all = df_all.reset_index(drop=True)
        return df_all


class YahooFinance:
    """

    """

    @classmethod
    async def yf_quotes_async(
            cls,
            ticker: str,
            history: str,
            interval: str) -> Quotes:
        param_history = {
            "ten_years": History.TEN_YEARS,
            "two_years": History.TWO_YEARS,
            "one_year": History.YEAR,
        }

        param_interval = {
            "day": Interval.DAY,
            "week": Interval.WEEK
        }

        result = await \
            OHLC.fetch(
                symbol=ticker,
                history=param_history.get(history),
                interval=param_interval.get(interval)
            )

        return result

    @classmethod
    def yf_quotes(cls, ticker: str, from_date: str, to_date: str, time_frame: str = 'd') -> pd.DataFrame:
        '''

        :param ticker:
        :param from_date:
        :param to_date:
        :param time_frame:
        :return: quotes from yahoo finance wrapper
        '''
        data = yf.download(ticker, from_date, to_date)
        data.reset_index(inplace=True)

        d = {
            "Date": 'datetime2',
            'Open': 'o',
            'High': 'h',
            "Low": "l",
            "Close": 'c',
            "Adj Close": "ac",
            "Volume": "v"
        }

        data.columns = data.columns.map(lambda col: d[col])
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
                ac=("ac", "last"),
                h=("h", np.max),
                l=("l", np.min),
                v=("v", np.sum)
            )
            ).reset_index(drop=True)

            data['datetime2'] = data['date_end']

            return data


class Finnhub:
    """

    """

    @classmethod
    def get_quotes_fh(self, ticker: str, start: int, end: int, token: str, freq=1):
        r = requests.get(
            f'https://finnhub.io/api/v1/stock/candle?symbol={ticker}&resolution={freq}&from={start}&to={end}&token={token}')
        return r.json()
