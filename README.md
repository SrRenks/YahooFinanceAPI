# Welcome to YFinance

An unofficial library to capture data from Yahoo Finance in an easier way. Having access to capture data from various actions provided by the service. It is possible to configure your capture to select and filter different types of data regarding the stocks provided.

# First Steps

initially, install the package through the following command in your terminal:

```
pip install git+https://github.com/SrRenks/yfinance.git
```

> **Note:** For those more familiarized and who choose to keep the packages in a venv environment, you can use the command **python -m venv yfinance**.

# Warnings

If you are running your code on **Windows**, it is extremely important that you consider indenting your code as in the following example:

```
from yfinance import YahooFinance

if __name__ == '__main__':
	# directly run your code that uses the
	# YahooFinance class here, or even functions that use it.
```

Then we would use it as follows:

```
from yfinance import YahooFinance

if __name__ == '__main__':
	stock_data = YahooFinance('STOCK_TICKER', '1d', '1d')
```

# Methods

Initially, it is necessary to instantiate our variable that will receive the **YahooFinance class**, followed by the following attributes:

| Attribute | Type                            |
| --------- | ------------------------------- |
| ticker    | **str** or **list** |
| interval  | **str**                   |
| range     | **str**                   |

* **ticker**: It is the code referring to the stock, pay attention to the **suffix** (if any), Example: many Brazilian stocks end with the suffix ".SA", it is recommended to check whether or not the stock you are looking for has a suffix directly on the **Yahoo Finance website**.
* **interval**: It is the **time interval** of how you want the data referring to the "range" attribute. Examples: 1m, 2m, 5m, 15m, 30m, 60m, 90m, 1h, 1d, 5d, 1wk, 1mo, 3mo.
* **range**: It is the scale of capture, which can be days, months or years. Examples: 1d, 2d, 5d, 1mo, 2mo, 3mo, 6mo, 1y, 2y, 5y, 10y, ytd, max.

For that, just use the method like this example code:

```
from yfinance import YahooFinance

google_data = YahooFinance('GOOG', '1d', '1d')
```

## info

Returns a **Pandas DataFrame**, with the following values referring to stock:

* chartPreviousClose
* currency
* currentTradingPeriod
* dataGranularity
* exchangeName
* exchangeTimezoneName
* firstTradeDate
* gmtoffset
* instrumentType
* previousClose
* priceHint
* range
* regularMarketPrice
* regularMarketTime
* scale
* symbol
* timezone

For that, just use the method like this example code:

```
from yfinance import YahooFinance

google_data = YahooFinance('GOOG', '1d', '1d')
google_info = google_data.info()
```

> **Note**: if you instantiate the **YahooFinance** class using a **list** of tickers for the "ticker" attribute, a **loading bar** will be shown referring to the amount of tickers present in the list.

## trading_period

Returns a **Pandas DataFrame** with the following data:

* timezone
* start
* end
* gmtoffset

 it is necessary to insert the argument referring to which **type** of period you are looking for, being possible to capture "**pre**", "**regular**" and "**post**".

| Arg  | Type |
| ---- | ---- |
| type | str  |

For that, just use the method like this example code:

```
from yfinance import YahooFinance

google_data = YahooFinance('GOOG', '1d', '1d')
google_trading_period = google_data.trading_period("regular")
```

> **Note**: if you instantiate the **YahooFinance** class using a **list** of tickers for the "ticker" attribute, a **loading bar** will be shown referring to the amount of tickers present in the list.

## all_values

Returns a **Pandas DataFrame** with the following data:

* ticker
* time
* close
* open
* high
* low
* volume

For that, just use the method like this example code:

```
from yfinance import YahooFinance

google_data = YahooFinance('GOOG', '1d', '1d')
google_values = google_data.trading_all_values()
```

> **Note**: if you instantiate the **YahooFinance** class using a **list** of tickers for the "ticker" attribute, a **loading bar** will be shown referring to the amount of tickers present in the list.

## close

Returns a **Pandas DataFrame** with the following data:

* ticker
* time
* close

For that, just use the method like this example code:

```
from yfinance import YahooFinance

google_data = YahooFinance('GOOG', '1d', '1d')
google_close = google_data.open()
```

## open

Returns a **Pandas DataFrame** with the following data:

* ticker
* time
* open

For that, just use the method like this example code:

```
from yfinance import YahooFinance

google_data = YahooFinance('GOOG', '1d', '1d')
google_open = google_data.open()
```

> **Note**: if you instantiate the **YahooFinance** class using a **list** of tickers for the "ticker" attribute, a **loading bar** will be shown referring to the amount of tickers present in the list.

## high

Returns a **Pandas DataFrame** with the following data:

* ticker
* time
* high

For that, just use the method like this example code:

```
from yfinance import YahooFinance

google_data = YahooFinance('GOOG', '1d', '1d')
google_high = google_data.high()
```

> **Note**: if you instantiate the **YahooFinance** class using a **list** of tickers for the "ticker" attribute, a **loading bar** will be shown referring to the amount of tickers present in the list.

## low

Returns a **Pandas DataFrame** with the following data:

* ticker
* time
* low

For that, just use the method like this example code:

```
from yfinance import YahooFinance

google_data = YahooFinance('GOOG', '1d', '1d')
google_low = google_data.low()
```

> **Note**: if you instantiate the **YahooFinance** class using a **list** of tickers for the "ticker" attribute, a **loading bar** will be shown referring to the amount of tickers present in the list.

## volume

Returns a **Pandas DataFrame** with the following data:

* ticker
* time
* volume

For that, just use the method like this example code:

```
from yfinance import YahooFinance

google_data = YahooFinance('GOOG', '1d', '1d')
google_volume = google_data.volume()
```

> **Note**: if you instantiate the **YahooFinance** class using a **list** of tickers for the "ticker" attribute, a **loading bar** will be shown referring to the amount of tickers present in the list.
>
