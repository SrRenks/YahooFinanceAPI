from multiprocessing import Pool, freeze_support, cpu_count
from yfapi.exceptions import NothingToExport, TickerNotFound
from datetime import datetime
from itertools import repeat
from tqdm import tqdm
import pandas as pd
import requests
import os

headers = {'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/105.0.0.0 Safari/537.36'}


def get_suffix(ticker):
    params = {'q': ticker, 'quotesCount': 1, 'newsCount': 0}
    try:
        data = requests.get("https://query2.finance.yahoo.com/v1/finance/search",
                            params=params, headers=headers).json()['quotes'][0]['symbol']
    except Exception as e:
        raise TickerNotFound(f"{ticker}: {e}")
    return data


def info(zip_args):
    ticker, interval, range = list(zip_args)
    info = requests.get(f'https://query1.finance.yahoo.com/v8/finance/chart/{get_suffix(ticker)}?interval={interval}&range={range}',
                        headers=headers).json()
    if info['chart']['error'] is None:
        info = info['chart']['result'][0]['meta']
        del info['currentTradingPeriod']
        del info['validRanges']
        del info['symbol']
        df = pd.DataFrame(info, index=[0])
        df.insert(loc=0, column='ticker', value=ticker.upper())
        return df
    else:
        raise ValueError(f'Error with ticker "{ticker.upper()}": {info["chart"]["error"]["description"]}.')


def trading_period(zip_args):
    ticker, type, interval, range = list(zip_args)
    if type not in ['pre', 'regular', 'post']:
        raise ValueError(f'Invalid type "{zip_args[1]}", valid types: "pre", "regular", "post"')
    data = requests.get(f'https://query1.finance.yahoo.com/v8/finance/chart/{get_suffix(ticker)}?interval={interval}&range={range}',
                        headers=headers).json()
    if data['chart']['error'] is None:
        data = data['chart']['result'][0]['meta']['currentTradingPeriod'][type]
        data = pd.DataFrame(data, index=[0])
        data.insert(loc=0, column='ticker', value=ticker.upper())
        return data
    else:
        raise ValueError(f'Error with ticker "{ticker.upper()}": {data["chart"]["error"]["description"]}.')


def all_values(zip_args):
    ticker, interval, range = list(zip_args)
    data = requests.get(f'https://query1.finance.yahoo.com/v8/finance/chart/{get_suffix(ticker)}?interval={interval}&range={range}',
                        headers=headers).json()
    if data['chart']['error'] is None:
        data = data['chart']['result'][0]
        df = pd.DataFrame(data=data['indicators']['quote'][0])
        time = pd.DataFrame(data=data['timestamp'], columns=['time'])
        time = time['time'].apply(lambda row: datetime.utcfromtimestamp(row).strftime('%d/%m/%Y %H:%M:%S'))
        df.insert(loc=0, column='date', value=time)
        df.insert(loc=0, column='ticker', value=ticker.upper())
        return df
    else:
        raise ValueError(f'Error with ticker "{ticker.upper()}": {data["chart"]["error"]["description"]}.')


def close(zip_args):
    ticker, interval, range = list(zip_args)
    data = requests.get(f'https://query1.finance.yahoo.com/v8/finance/chart/{get_suffix(ticker)}?interval={interval}&range={range}',
                        headers=headers).json()
    if data['chart']['error'] is None:
        data = data['chart']['result'][0]
        df = pd.DataFrame(data=data['indicators']['quote']
                          [0]['close'], index=None, columns=['close'])
        time = pd.DataFrame(data=data['timestamp'], columns=['time'])
        time = time['time'].apply(lambda row: datetime.utcfromtimestamp(row).strftime('%d/%m/%Y %H:%M:%S'))
        df.insert(loc=0, column='date', value=time)
        df.insert(loc=0, column='ticker', value=ticker.upper())
        return df
    else:
        raise ValueError(f'Error with ticker "{ticker.upper()}": {data["chart"]["error"]["description"]}.')


def open(zip_args):
    ticker, interval, range = list(zip_args)
    data = requests.get(f'https://query1.finance.yahoo.com/v8/finance/chart/{get_suffix(ticker)}?interval={interval}&range={range}',
                        headers=headers).json()
    if data['chart']['error'] is None:
        data = data['chart']['result'][0]
        df = pd.DataFrame(data=data['indicators']['quote']
                          [0]['open'], index=None, columns=['open'])
        time = pd.DataFrame(data=data['timestamp'], columns=['time'])
        time = time['time'].apply(lambda row: datetime.utcfromtimestamp(row).strftime('%d/%m/%Y %H:%M:%S'))
        df.insert(loc=0, column='date', value=time)
        df.insert(loc=0, column='ticker', value=ticker.upper())
        return df
    else:
        raise ValueError(f'Error with ticker "{ticker.upper()}": {data["chart"]["error"]["description"]}.')


def high(zip_args):
    ticker, interval, range = list(zip_args)
    data = requests.get(f'https://query1.finance.yahoo.com/v8/finance/chart/{get_suffix(ticker)}?interval={interval}&range={range}',
                        headers=headers).json()
    if data['chart']['error'] is None:
        data = data['chart']['result'][0]
        df = pd.DataFrame(data=data['indicators']['quote']
                          [0]['high'], index=None, columns=['high'])
        time = pd.DataFrame(data=data['timestamp'], columns=['time'])
        time = time['time'].apply(lambda row: datetime.utcfromtimestamp(row).strftime('%d/%m/%Y %H:%M:%S'))
        df.insert(loc=0, column='date', value=time)
        df.insert(loc=0, column='ticker', value=ticker.upper())
        return df
    else:
        raise ValueError(f'Error with ticker "{ticker.upper()}": {data["chart"]["error"]["description"]}.')


def low(zip_args):
    ticker, interval, range = list(zip_args)
    data = requests.get(f'https://query1.finance.yahoo.com/v8/finance/chart/{get_suffix(ticker)}?interval={interval}&range={range}',
                        headers=headers).json()
    if data['chart']['error'] is None:
        data = data['chart']['result'][0]
        df = pd.DataFrame(data=data['indicators']['quote']
                          [0]['low'], index=None, columns=['low'])
        time = pd.DataFrame(data=data['timestamp'], columns=['time'])
        time = time['time'].apply(lambda row: datetime.utcfromtimestamp(row).strftime('%d/%m/%Y %H:%M:%S'))
        df.insert(loc=0, column='date', value=time)
        df.insert(loc=0, column='ticker', value=ticker.upper())
        return df
    else:
        raise ValueError(f'Error with ticker "{ticker.upper()}": {data["chart"]["error"]["description"]}.')


def volume(zip_args):
    ticker, interval, range = list(zip_args)
    data = requests.get(f'https://query1.finance.yahoo.com/v8/finance/chart/{get_suffix(ticker)}?interval={interval}&range={range}',
                        headers=headers).json()
    if data['chart']['error'] is None:
        data = data['chart']['result'][0]
        df = pd.DataFrame(data=data['indicators']['quote']
                          [0]['volume'], index=None, columns=['volume'])
        time = pd.DataFrame(data=data['timestamp'], columns=['time'])
        time = time['time'].apply(lambda row: datetime.utcfromtimestamp(row).strftime('%d/%m/%Y %H:%M:%S'))
        df.insert(loc=0, column='date', value=time)
        df.insert(loc=0, column='ticker', value=ticker.upper())
        return df
    else:
        raise ValueError(f'Error with ticker "{ticker.upper()}": {data["chart"]["error"]["description"]}.')


class YahooFinance():
    def __init__(self, ticker, interval, range):
        if interval.lower() not in ['1m', '2m', '5m', '15m', '30m', '60m', '90m', '1h', '1d', '5d', '1wk', '1mo', '3mo']:
            raise ValueError(f'invalid interval "{interval}", valid intervals: 1m, 2m, 5m, 15m, 30m, 60m, 90m, 1h, 1d, 5d, 1wk, 1mo, 3mo.')
        if range.lower() not in ["1d", "2d", "5d", "1mo", "2mo", "3mo", "6mo", "1y", "2y", "5y", "10y", "ytd", "max"]:
            raise ValueError(f'invalid range "{range}", valid ranges: 1d, 5d, 1mo, 3mo, 6mo, 1y, 2y, 5y, 10y, ytd, max')
        if isinstance(ticker, list):
            if False in [True if isinstance(item, str) else False for item in ticker]:
                raise ValueError("all tickers must be strings.")
        self.__ticker = ticker
        self.__interval = interval.lower()
        self.__range = range.lower()
        self.__data = None

    def info(self):
        if isinstance(self.__ticker, str):
            data = info(((self.__ticker, self.__interval, self.__range)))
            self.__data = data
            return self.__data
        elif isinstance(self.__ticker, list):
            dataframes = []
            with Pool(cpu_count()) as pool:
                for dataframe in tqdm(pool.imap_unordered(info, zip(self.__ticker, repeat(self.__interval), repeat(self.__range))), total=len(self.__ticker), leave=False, ascii=' ━', colour='GREEN'):
                    dataframes.append(dataframe)
            data = pd.concat(dataframes)
            data = data.reset_index(drop=True)
            self.__data = data
            return data

    def trading_period(self, type):
        if isinstance(self.__ticker, str):
            data = trading_period((self.__ticker, type.lower(), self.__interval, self.__range))
            self.__data = data
            return self.__data
        if isinstance(self.__ticker, list):
            dataframes = []
            with Pool(cpu_count()) as pool:
                for dataframe in tqdm(pool.imap_unordered(trading_period, zip(self.__ticker, repeat(type), repeat(self.__interval), repeat(self.__range))), total=len(self.__ticker), leave=False, ascii=' ━', colour='GREEN'):
                    dataframes.append(dataframe)
            data = pd.concat(dataframes)
            data = data.reset_index(drop=True)
            self.__data = data
            return data

    def all_values(self):
        if isinstance(self.__ticker, str):
            data = all_values(((self.__ticker, self.__interval, self.__range)))
            self.__data = data
            return self.__data
        elif isinstance(self.__ticker, list):
            dataframes = []
            with Pool(cpu_count()) as pool:
                for dataframe in tqdm(pool.imap_unordered(all_values, zip(self.__ticker, repeat(self.__interval), repeat(self.__range))), total=len(self.__ticker), leave=False, ascii=' ━', colour='GREEN'):
                    dataframes.append(dataframe)
            data = pd.concat(dataframes)
            data = data.reset_index(drop=True)
            self.__data = data
            return data

    def close(self):
        if isinstance(self.__ticker, str):
            data = close(((self.__ticker, self.__interval, self.__range)))
            self.__data = data
            return self.__data
        elif isinstance(self.__ticker, list):
            dataframes = []
            with Pool(cpu_count()) as pool:
                for dataframe in tqdm(pool.imap_unordered(close, zip(self.__ticker, repeat(self.__interval), repeat(self.__range))), total=len(self.__ticker), leave=False, ascii=' ━', colour='GREEN'):
                    dataframes.append(dataframe)
            data = pd.concat(dataframes)
            data = data.reset_index(drop=True)
            self.__data = data
            return data

    def open(self):
        if isinstance(self.__ticker, str):
            data = open(((self.__ticker, self.__interval, self.__range)))
            self.__data = data
            return self.__data
        elif isinstance(self.__ticker, list):
            dataframes = []
            with Pool(cpu_count()) as pool:
                for dataframe in tqdm(pool.imap_unordered(open, zip(self.__ticker, repeat(self.__interval), repeat(self.__range))), total=len(self.__ticker), leave=False, ascii=' ━', colour='GREEN'):
                    dataframes.append(dataframe)
            data = pd.concat(dataframes)
            data = data.reset_index(drop=True)
            self.__data = data
            return data

    def high(self):
        if isinstance(self.__ticker, str):
            data = high(((self.__ticker, self.__interval, self.__range)))
            self.__data = data
            return self.__data
        elif isinstance(self.__ticker, list):
            dataframes = []
            with Pool(cpu_count()) as pool:
                for dataframe in tqdm(pool.imap_unordered(high, zip(self.__ticker, repeat(self.__interval), repeat(self.__range))), total=len(self.__ticker), leave=False, ascii=' ━', colour='GREEN'):
                    dataframes.append(dataframe)
            data = pd.concat(dataframes)
            data = data.reset_index(drop=True)
            self.__data = data
            return data

    def low(self):
        if isinstance(self.__ticker, str):
            data = low(((self.__ticker, self.__interval, self.__range)))
            self.__data = data
            return self.__data
        elif isinstance(self.__ticker, list):
            dataframes = []
            with Pool(cpu_count()) as pool:
                for dataframe in tqdm(pool.imap_unordered(low, zip(self.__ticker, repeat(self.__interval), repeat(self.__range))), total=len(self.__ticker), leave=False, ascii=' ━', colour='GREEN'):
                    dataframes.append(dataframe)
            data = pd.concat(dataframes)
            data = data.reset_index(drop=True)
            self.__data = data
            return data

    def volume(self):
        if isinstance(self.__ticker, str):
            data = volume(((self.__ticker, self.__interval, self.__range)))
            self.__data = data
            return self.__data
        elif isinstance(self.__ticker, list):
            dataframes = []
            with Pool(cpu_count()) as pool:
                for dataframe in tqdm(pool.imap_unordered(volume, zip(self.__ticker, repeat(self.__interval), repeat(self.__range))), total=len(self.__ticker), leave=False, ascii=' ━', colour='GREEN'):
                    dataframes.append(dataframe)
            data = pd.concat(dataframes)
            data = data.reset_index(drop=True)
            self.__data = data
            return data

    def to_csv(self, dir='database.csv', sep=';'):
        if isinstance(self.__data, pd.DataFrame):
            if dir != 'database.csv':
                if not '.csv' in dir[-4:]:
                    dir += '.csv'
            self.__data.to_csv(dir, sep=sep, index=None)
        else:
            raise NothingToExport('No data found to export')

    def to_excel(self, dir='database.xlsx', sep=';'):
        if isinstance(self.__data, pd.DataFrame):
            if dir != 'database.xlsx':
                if not '.xlsx' in dir[-4:]:
                    dir += '.xlsx'
                else:
                    dir = dir
            self.__data.to_excel(dir, index=None)
        else:
            raise NothingToExport('No data found to export')

if __name__ != '__main__':
    if os.name == 'nt':
        freeze_support()
