from multiprocessing import Pool
from datetime import datetime
from itertools import repeat
import pandas as pd
import requests


headers = {'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/105.0.0.0 Safari/537.36'}


def info(ticker, interval, range):
    info = requests.get(f'https://query1.finance.yahoo.com/v8/finance/chart/{ticker.upper()}?interval={interval}&range={range}', headers=headers).json()
    if info['chart']['error'] is None:
        info = info['chart']['result'][0]['meta']
        del info['currentTradingPeriod']
        del info['validRanges']
        df = pd.DataFrame(info, index=[0])
        df.insert(loc=0, column='ticker', value=ticker)
        return df
    else:
        raise ValueError(f'Error with ticker "{ticker}": {info["chart"]["error"]["description"]}.')


def multi_info(ticker_list, interval, range):
    with Pool() as p:
        data = p.starmap(info, zip(ticker_list, repeat(interval), repeat(range)))
        data = pd.concat(data)
        return data.reset_index(drop=True)


def trading_period(ticker, type, interval, range):
    if type not in ['pre', 'regular', 'post']:
        raise ValueError(f'Invalid type "{type}", valid types: "pre", "regular", "post"')
    trading_period = requests.get(f'https://query1.finance.yahoo.com/v8/finance/chart/{ticker.upper()}?interval={interval}&range={range}', headers=headers).json()
    if trading_period['chart']['error'] is None:
        trading_period = trading_period['chart']['result'][0]['meta']['currentTradingPeriod'][type]
        trading_period = pd.DataFrame(trading_period, index=[0])
        trading_period.insert(loc=0, column='ticker', value=ticker)
        return trading_period
    else:
        raise ValueError(f'Error with ticker "{ticker}": {trading_period["chart"]["error"]["description"]}.')


def multi_trading_period(ticker_list, type, interval, range):
    with Pool() as p:
        data = p.starmap(trading_period, zip(ticker_list, repeat(type), repeat(interval), repeat(range)))
        data = pd.concat(data)
        return data.reset_index(drop=True)


def all_values(ticker, interval, range):
    data = requests.get(f'https://query1.finance.yahoo.com/v8/finance/chart/{ticker.upper()}?interval={interval}&range={range}', headers=headers).json()
    if data['chart']['error'] is None:
        data = data['chart']['result'][0]
        df = pd.DataFrame(data=data['indicators']['quote'][0])
        time = pd.DataFrame(data=data['timestamp'], columns=['time'])
        time = time['time'].apply(lambda row: datetime.utcfromtimestamp(row).strftime('%d/%m/%Y %H:%M:%S'))
        df.insert(loc=0, column='time', value=time)
        df.insert(loc=0, column='ticker', value=ticker)
        return df
    else:
        raise ValueError(f'Error with ticker "{ticker}": {data["chart"]["error"]["description"]}.')


def multi_all_values(ticker_list, interval, range):
    with Pool() as p:
        data = p.starmap(all_values, zip(ticker_list, repeat(interval), repeat(range)))
        data = pd.concat(data)
        return data.reset_index(drop=True)


def close(ticker, interval, range):
    data = requests.get(f'https://query1.finance.yahoo.com/v8/finance/chart/{ticker}?interval={interval}&range={range}', headers=headers).json()
    if data['chart']['error'] is None:
        data = data['chart']['result'][0]
        df = pd.DataFrame(data=data['indicators']['quote'][0]['close'], index=None, columns=['close'])
        time = pd.DataFrame(data=data['timestamp'], columns=['time'])
        time = time['time'].apply(lambda row: datetime.utcfromtimestamp(row).strftime('%d/%m/%Y %H:%M:%S'))
        df.insert(loc=0, column='time', value=time)
        df.insert(loc=0, column='ticker', value=ticker)
        return df
    else:
        raise ValueError(f'Error with ticker "{ticker}": {data["chart"]["error"]["description"]}.')


def multi_close(ticker_list, interval, range):
    with Pool() as p:
        data = p.starmap(close, zip(ticker_list, repeat(interval), repeat(range)))
        data = pd.concat(data)
        return data.reset_index(drop=True)


def open(ticker, interval, range):
    data = requests.get(f'https://query1.finance.yahoo.com/v8/finance/chart/{ticker}?interval={interval}&range={range}', headers=headers).json()
    if data['chart']['error'] is None:
        data = data['chart']['result'][0]
        df = pd.DataFrame(data=data['indicators']['quote'][0]['open'], index=None, columns=['open'])
        time = pd.DataFrame(data=data['timestamp'], columns=['time'])
        time = time['time'].apply(lambda row: datetime.utcfromtimestamp(row).strftime('%d/%m/%Y %H:%M:%S'))
        df.insert(loc=0, column='time', value=time)
        df.insert(loc=0, column='ticker', value=ticker)
        return df
    else:
        raise ValueError(f'Error with ticker "{ticker}": {data["chart"]["error"]["description"]}.')


def multi_open(ticker_list, interval, range):
    with Pool() as p:
        data = p.starmap(open, zip(ticker_list, repeat(interval), repeat(range)))
        data = pd.concat(data)
        return data.reset_index(drop=True)


def high(ticker, interval, range):
    data = requests.get(f'https://query1.finance.yahoo.com/v8/finance/chart/{ticker}?interval={interval}&range={range}', headers=headers).json()
    if data['chart']['error'] is None:
        data = requests.get(f'https://query1.finance.yahoo.com/v8/finance/chart/{ticker}?interval={interval}&range={range}', headers=headers).json()
        data = data['chart']['result'][0]
        df = pd.DataFrame(data=data['indicators']['quote'][0]['high'], index=None, columns=['high'])
        time = pd.DataFrame(data=data['timestamp'], columns=['time'])
        time = time['time'].apply(lambda row: datetime.utcfromtimestamp(row).strftime('%d/%m/%Y %H:%M:%S'))
        df.insert(loc=0, column='time', value=time)
        df.insert(loc=0, column='ticker', value=ticker)
        return df
    else:
        raise ValueError(f'Error with ticker "{ticker}": {data["chart"]["error"]["description"]}.')


def multi_high(ticker_list, interval, range):
    with Pool() as p:
        data = p.starmap(high, zip(ticker_list, repeat(interval), repeat(range)))
        data = pd.concat(data)
        return data.reset_index(drop=True)


def low(ticker, interval, range):
    data = requests.get(f'https://query1.finance.yahoo.com/v8/finance/chart/{ticker}?interval={interval}&range={range}', headers=headers).json()
    if data['chart']['error'] is None:
        data = requests.get(f'https://query1.finance.yahoo.com/v8/finance/chart/{ticker}?interval={interval}&range={range}', headers=headers).json()
        data = data['chart']['result'][0]
        df = pd.DataFrame(data=data['indicators']['quote'][0]['low'], index=None, columns=['low'])
        time = pd.DataFrame(data=data['timestamp'], columns=['time'])
        time = time['time'].apply(lambda row: datetime.utcfromtimestamp(row).strftime('%d/%m/%Y %H:%M:%S'))
        df.insert(loc=0, column='time', value=time)
        df.insert(loc=0, column='ticker', value=ticker)
        return df
    else:
        raise ValueError(f'Error with ticker "{ticker}": {data["chart"]["error"]["description"]}.')


def multi_low(ticker_list, interval, range):
    with Pool() as p:
        data = p.starmap(low, zip(ticker_list, repeat(interval), repeat(range)))
        data = pd.concat(data)
        return data.reset_index(drop=True)


def volume(ticker, interval, range):
    data = requests.get(f'https://query1.finance.yahoo.com/v8/finance/chart/{ticker}?interval={interval}&range={range}', headers=headers).json()
    if data['chart']['error'] is None:
        data = requests.get(f'https://query1.finance.yahoo.com/v8/finance/chart/{ticker}?interval={interval}&range={range}', headers=headers).json()
        data = data['chart']['result'][0]
        df = pd.DataFrame(data=data['indicators']['quote'][0]['volume'], index=None, columns=['volume'])
        time = pd.DataFrame(data=data['timestamp'], columns=['time'])
        time = time['time'].apply(lambda row: datetime.utcfromtimestamp(row).strftime('%d/%m/%Y %H:%M:%S'))
        df.insert(loc=0, column='time', value=time)
        df.insert(loc=0, column='ticker', value=ticker)
        return df
    else:
        raise ValueError(f'Error with ticker "{ticker}": {data["chart"]["error"]["description"]}.')


def multi_volume(ticker_list, interval, range):
    with Pool() as p:
        data = p.starmap(volume, zip(ticker_list, repeat(interval), repeat(range)))
        data = pd.concat(data)
        return data.reset_index(drop=True)


class YahooFinance():
    def __init__(self, ticker, interval, range):
        if interval.lower() not in ['1m', '2m', '5m', '15m', '30m', '60m', '90m', '1h', '1d', '5d', '1wk', '1mo', '3mo']:
            raise ValueError(f'invalid interval "{interval}", valid intervals: 1m, 2m, 5m, 15m, 30m, 60m, 90m, 1h, 1d, 5d, 1wk, 1mo, 3mo.')
        if range.lower() not in ["1d", "5d", "1mo", "3mo", "6mo", "1y", "2y", "5y", "10y", "ytd", "max"]:
            raise ValueError(f'invalid range "{range}", valid ranges: 1d, 5d, 1mo, 3mo, 6mo, 1y, 2y, 5y, 10y, ytd, max')
        if isinstance(ticker, list):
            if False in [True if isinstance(item, str) else False for item in ticker]:
                raise ValueError("all tickers must be strings.")
        self.ticker = ticker
        self.interval = interval.lower()
        self.range = range.lower()

    def info(self):
        if isinstance(self.ticker, str):
            return info(self.ticker, self.interval, self.range)
        elif isinstance(self.ticker, list):
            return multi_info(self.ticker, self.interval, self.range)

    def trading_period(self, type):
        if isinstance(self.ticker, str):
            return trading_period(self.ticker, type, self.interval, self.range)
        if isinstance(self.ticker, list):
            return multi_trading_period(self.ticker, type, self.interval, self.range)

    def all_values(self):
        if isinstance(self.ticker, str):
            return all_values(self.ticker, self.interval, self.range)
        elif isinstance(self.ticker, list):
            return multi_all_values(self.ticker, self.interval, self.range)

    def close(self):
        if isinstance(self.ticker, str):
            return close(self.ticker, self.interval, self.range)
        elif isinstance(self.ticker, list):
            return multi_close(self.ticker, self.interval, self.range)

    def open(self):
        if isinstance(self.ticker, str):
            return open(self.ticker, self.interval, self.range)
        elif isinstance(self.ticker, list):
            return multi_open(self.ticker, self.interval, self.range)

    def high(self):
        if isinstance(self.ticker, str):
            return high(self.ticker, self.interval, self.range)
        elif isinstance(self.ticker, list):
            return multi_high(self.ticker, self.interval, self.range)

    def low(self):
        if isinstance(self.ticker, str):
            return low(self.ticker, self.interval, self.range)
        elif isinstance(self.ticker, list):
            return multi_low(self.ticker, self.interval, self.range)

    def volume(self):
        if isinstance(self.ticker, str):
            return volume(self.ticker, self.interval, self.range)
        elif isinstance(self.ticker, list):
            return multi_volume(self.ticker, self.interval, self.range)
