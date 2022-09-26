from multiprocessing import Pool
from datetime import datetime
from itertools import repeat
from tqdm import tqdm
import pandas as pd
import requests

headers = {'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/105.0.0.0 Safari/537.36'}

def info(zip_args):
    ticker, interval, range = list(zip_args)
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


def trading_period(zip_args):
    ticker, type, interval, range = list(zip_args)
    if zip_args[1] not in ['pre', 'regular', 'post']:
        raise ValueError(f'Invalid type "{zip_args[1]}", valid types: "pre", "regular", "post"')
    data = requests.get(f'https://query1.finance.yahoo.com/v8/finance/chart/{ticker.upper()}?interval={interval}&range={range}', headers=headers).json()
    if data['chart']['error'] is None:
        data = data['chart']['result'][0]['meta']['currentTradingPeriod'][type]
        data = pd.DataFrame(data, index=[0])
        data.insert(loc=0, column='ticker', value=ticker)
        return data
    else:
        raise ValueError(f'Error with ticker "{ticker}": {data["chart"]["error"]["description"]}.')


def all_values(zip_args):
    ticker, interval, range = list(zip_args)
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


def close(zip_args):
    ticker, interval, range = list(zip_args)
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


def open(zip_args):
    ticker, interval, range = list(zip_args)
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


def high(zip_args):
    ticker, interval, range = list(zip_args)
    data = requests.get(f'https://query1.finance.yahoo.com/v8/finance/chart/{ticker}?interval={interval}&range={range}', headers=headers).json()
    if data['chart']['error'] is None:
        data = data['chart']['result'][0]
        df = pd.DataFrame(data=data['indicators']['quote'][0]['high'], index=None, columns=['high'])
        time = pd.DataFrame(data=data['timestamp'], columns=['time'])
        time = time['time'].apply(lambda row: datetime.utcfromtimestamp(row).strftime('%d/%m/%Y %H:%M:%S'))
        df.insert(loc=0, column='time', value=time)
        df.insert(loc=0, column='ticker', value=ticker)
        return df
    else:
        raise ValueError(f'Error with ticker "{ticker}": {data["chart"]["error"]["description"]}.')


def low(zip_args):
    ticker, interval, range = list(zip_args)
    data = requests.get(f'https://query1.finance.yahoo.com/v8/finance/chart/{ticker}?interval={interval}&range={range}', headers=headers).json()
    if data['chart']['error'] is None:
        data = data['chart']['result'][0]
        df = pd.DataFrame(data=data['indicators']['quote'][0]['low'], index=None, columns=['low'])
        time = pd.DataFrame(data=data['timestamp'], columns=['time'])
        time = time['time'].apply(lambda row: datetime.utcfromtimestamp(row).strftime('%d/%m/%Y %H:%M:%S'))
        df.insert(loc=0, column='time', value=time)
        df.insert(loc=0, column='ticker', value=ticker)
        return df
    else:
        raise ValueError(f'Error with ticker "{ticker}": {data["chart"]["error"]["description"]}.')


def volume(zip_args):
    ticker, interval, range = list(zip_args)
    data = requests.get(f'https://query1.finance.yahoo.com/v8/finance/chart/{ticker}?interval={interval}&range={range}', headers=headers).json()
    if data['chart']['error'] is None:
        data = data['chart']['result'][0]
        df = pd.DataFrame(data=data['indicators']['quote'][0]['volume'], index=None, columns=['volume'])
        time = pd.DataFrame(data=data['timestamp'], columns=['time'])
        time = time['time'].apply(lambda row: datetime.utcfromtimestamp(row).strftime('%d/%m/%Y %H:%M:%S'))
        df.insert(loc=0, column='time', value=time)
        df.insert(loc=0, column='ticker', value=ticker)
        return df
    else:
        raise ValueError(f'Error with ticker "{ticker}": {data["chart"]["error"]["description"]}.')


class YahooFinance():
    def __init__(self, ticker, interval, range):
        if interval.lower() not in ['1m', '2m', '5m', '15m', '30m', '60m', '90m', '1h', '1d', '5d', '1wk', '1mo', '3mo']:
            raise ValueError(f'invalid interval "{interval}", valid intervals: 1m, 2m, 5m, 15m, 30m, 60m, 90m, 1h, 1d, 5d, 1wk, 1mo, 3mo.')
        if range.lower() not in ["1d", "2d", "5d", "1mo", "2mo", "3mo", "6mo", "1y", "2y", "5y", "10y", "ytd", "max"]:
            raise ValueError(f'invalid range "{range}", valid ranges: 1d, 5d, 1mo, 3mo, 6mo, 1y, 2y, 5y, 10y, ytd, max')
        if isinstance(ticker, list):
            if False in [True if isinstance(item, str) else False for item in ticker]:
                raise ValueError("all tickers must be strings.")
        self.ticker = ticker
        self.interval = interval.lower()
        self.range = range.lower()

    def info(self):
        if isinstance(self.ticker, str):
            return info(((self.ticker, self.interval, self.range)))
        elif isinstance(self.ticker, list):
            dataframes = []
            with Pool() as pool:
                for dataframe in tqdm(pool.imap_unordered(info, zip(self.ticker, repeat(self.interval), repeat(self.range))), total=len(self.ticker), leave=False):
                    dataframes.append(dataframe)
            data = pd.concat(dataframes)
            return data.reset_index(drop=True)

    def trading_period(self, type):
        if isinstance(self.ticker, str):
            return trading_period((self.ticker, type, self.interval, self.range))
        if isinstance(self.ticker, list):
            dataframes = []
            with Pool() as pool:
                for dataframe in tqdm(pool.imap_unordered(trading_period, zip(self.ticker, repeat(type), repeat(self.interval), repeat(self.range))), total=len(self.ticker), leave=False):
                    dataframes.append(dataframe)
            data = pd.concat(dataframes)
            return data.reset_index(drop=True)

    def all_values(self):
        if isinstance(self.ticker, str):
            return all_values(((self.ticker, self.interval, self.range)))
        elif isinstance(self.ticker, list):
            dataframes = []
            with Pool() as pool:
                for dataframe in tqdm(pool.imap_unordered(all_values, zip(self.ticker, repeat(self.interval), repeat(self.range))), total=len(self.ticker), leave=False):
                    dataframes.append(dataframe)
            data = pd.concat(dataframes)
            return data.reset_index(drop=True)

    def close(self):
        if isinstance(self.ticker, str):
            return close(((self.ticker, self.interval, self.range)))
        elif isinstance(self.ticker, list):
            dataframes = []
            with Pool() as pool:
                for dataframe in tqdm(pool.imap_unordered(close, zip(self.ticker, repeat(self.interval), repeat(self.range))), total=len(self.ticker), leave=False):
                    dataframes.append(dataframe)
            data = pd.concat(dataframes)
            return data.reset_index(drop=True)

    def open(self):
        if isinstance(self.ticker, str):
            return open(((self.ticker, self.interval, self.range)))
        elif isinstance(self.ticker, list):
            dataframes = []
            with Pool() as pool:
                for dataframe in tqdm(pool.imap_unordered(open, zip(self.ticker, repeat(self.interval), repeat(self.range))), total=len(self.ticker), leave=False):
                    dataframes.append(dataframe)
            data = pd.concat(dataframes)
            return data.reset_index(drop=True)

    def high(self):
        if isinstance(self.ticker, str):
            return high(((self.ticker, self.interval, self.range)))
        elif isinstance(self.ticker, list):
            dataframes = []
            with Pool() as pool:
                for dataframe in tqdm(pool.imap_unordered(high, zip(self.ticker, repeat(self.interval), repeat(self.range))), total=len(self.ticker), leave=False):
                    dataframes.append(dataframe)
            data = pd.concat(dataframes)
            return data.reset_index(drop=True)

    def low(self):
        if isinstance(self.ticker, str):
            return low(((self.ticker, self.interval, self.range)))
        elif isinstance(self.ticker, list):
            dataframes = []
            with Pool() as pool:
                for dataframe in tqdm(pool.imap_unordered(low, zip(self.ticker, repeat(self.interval), repeat(self.range))), total=len(self.ticker), leave=False):
                    dataframes.append(dataframe)
            data = pd.concat(dataframes)
            return data.reset_index(drop=True)

    def volume(self):
        if isinstance(self.ticker, str):
            return volume(((self.ticker, self.interval, self.range)))
        elif isinstance(self.ticker, list):
            dataframes = []
            with Pool() as pool:
                for dataframe in tqdm(pool.imap_unordered(volume, zip(self.ticker, repeat(self.interval), repeat(self.range))), total=len(self.ticker), leave=False):
                    dataframes.append(dataframe)
            data = pd.concat(dataframes)
            return data.reset_index(drop=True)
