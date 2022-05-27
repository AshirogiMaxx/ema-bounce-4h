import pandas as pd
import numpy as np
import requests.sessions
import json
from threading import Thread
import time
from influxdb import InfluxDBClient
from binance.client import Client
import sys
from time import perf_counter
from binance import AsyncClient
from datetime import datetime
import config

import logging

FORMAT = '%(levelname)s: %(asctime)s | FILE: %(filename)s:%(lineno)d | MESSAGE: %(message)s'

#logging.basicConfig(level=logging.INFO, format = FORMAT, datefmt="%H:%M:%S", stream=sys.stderr)
logging.basicConfig(
    format=FORMAT,
    filename = 'log.log',
    level=logging.INFO,
)
logger = logging.getLogger("- EMA BOUNCE -: ")

api_key = config.api_key
api_secret = config.api_secret
client = Client(api_key, api_secret)

client.session.mount('https://', requests.adapters.HTTPAdapter(pool_maxsize = 400))


pd.set_option("display.max_rows", 1500)

symbols = []
candles = {}
above50 = []
prices = {}
TIMEFRAME = '1h'

def job():
    print('Starting...')
    print(extract_candles())

def add_EMA(price, day):
    return price.ewm(span=day, adjust=False).mean()

def add_MA(price, day):
    return price.rolling(window = day).mean()

def add_STOCH(close, low, high, period, k, d=0):
    STOCH_K = ((close - low.rolling(window=period).min()) / (high.rolling(window=period).max() - low.rolling(window=period).min())) * 100
    STOCH_K = STOCH_K.rolling(window=k).mean()
    if d == 0:
      return STOCH_K
    else:
      STOCH_D = STOCH_K.rolling(window=d).mean()
      return STOCH_D


def computeRSI(data, time_window):
    diff = data.diff(1).dropna()  # diff in one field(one day)

    # this preservers dimensions off diff values
    up_chg = 0 * diff
    down_chg = 0 * diff

    # up change is equal to the positive difference, otherwise equal to zero
    up_chg[diff > 0] = diff[diff > 0]

    # down change is equal to negative deifference, otherwise equal to zero
    down_chg[diff < 0] = diff[diff < 0]

    # check pandas documentation for ewm
    # https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.ewm.html
    # values are related to exponential decay
    # we set com=time_window-1 so we get decay alpha=1/time_window
    up_chg_avg = up_chg.ewm(com=time_window - 1, min_periods=time_window).mean()
    down_chg_avg = down_chg.ewm(com=time_window - 1, min_periods=time_window).mean()

    rs = abs(up_chg_avg / down_chg_avg)
    rsi = 100 - 100 / (1 + rs)
    return rsi


def stochastic(data, k_window, d_window, window):
    # input to function is one column from df
    # containing closing price or whatever value we want to extract K and D from

    min_val = data.rolling(window=window, center=False).min()
    max_val = data.rolling(window=window, center=False).max()

    stoch = ((data - min_val) / (max_val - min_val)) * 100

    K = stoch.rolling(window=k_window, center=False).mean()
    # K = stoch

    D = K.rolling(window=d_window, center=False).mean()

    return K, D

def load_candles(sym):
    global candles
    #coin_klines = client.futures_historical_klines(sym, client.KLINE_INTERVAL_15MINUTE, "10 day ago UTC") # 15 minute candles
    #coin_klines = client.futures_historical_klines(sym, client.KLINE_INTERVAL_1HOUR, "34 day ago UTC")  # 1 hour candles
    #coin_klines = client.get_historical_klines(sym, client.KLINE_INTERVAL_1HOUR, "34 day ago UTC") # 1 hour candles
    coin_klines = client.get_historical_klines(sym, client.KLINE_INTERVAL_4HOUR, "150 day ago UTC") # 4 hour candles targeting BTC pairs

    #klines = json.loads(resp.content)

    parsed_klines=[]
    for k in coin_klines:
        k_candle = {
            '': float(k[0]),
            'Date': float(k[0]),
            'Open': float(k[1]),
            'High': float(k[2]),
            'Low': float(k[3]),
            'Close': float(k[4]),
            'Volume': float(k[5])
        }
        parsed_klines.append(k_candle)
    candles[sym] = parsed_klines
    #logger.info("Found %d USDT klines", len(parsed_klines))

def calculate_ema():
    global prices
    for pair in candles:

        if candles[pair]:
            data = pd.read_json(json.dumps(candles[pair]), convert_dates=False, convert_axes=False, orient='records')
        else:
            continue
        #-data.name = sym

        prices[pair] = {}
        close = data['Close']
        low = data['Low']
        high = data['High']
        data['MA200'] = add_MA(close, 200)
        sma200 = data['MA200']
        modPrice = data['Close'].copy()
        modPrice2 = data['Close'].copy()
        modPrice.iloc[0:200] = sma200[0:200]
        modPrice2.iloc[0:89] = sma200[0:89]
        data['EMA-200'] = add_EMA(modPrice, 200)
        data['EMA-89'] = add_EMA(modPrice2, 89)
        prices[pair]['ema200'] = data.iloc[-2]['EMA-200']
        prices[pair]['ma200'] = data.iloc[-2]['MA200']
        prices[pair]['ema89'] = data.iloc[-2]['EMA-89']
        prices[pair]['close_price'] = candles[pair][-2]['Close']  # save current price
        prices[pair]['open_price'] = candles[pair][-2]['Open']  # save current price
        prices[pair]['high_price'] = candles[pair][-2]['High']  # save current price
        prices[pair]['low_price'] = candles[pair][-2]['Low']  # save current price
        data['RSI'] = computeRSI(data['Close'], 14)
        data['STOCH_%K(14,3,3)'], data['STOCH_%D(14,3,3)'] = stochastic(data['RSI'], 3, 3, 14)
        #data['STOCH_%D(14,3,3)'] = add_STOCH(close, low, high, 14, 3, 3)
        prices[pair]['stoch_k'] = data.iloc[-2]['STOCH_%K(14,3,3)']
        prices[pair]['stoch_d'] = data.iloc[-2]['STOCH_%D(14,3,3)']

def store_data(data, duration, engine, exchange_name, market):
    # Storing data
    json_body = []
    for candle in data:
        json_body.append({
            "measurement": exchange_name,
            "tags": {
                "duration": duration,
                "market": market
            },
            "time": candle[0],
            "fields": {
                "open": candle[1] + 0.0,
                "high": candle[2] + 0.0,
                "low": candle[3] + 0.0,
                "close": candle[4] + 0.0,
                "volume": candle[5] + 0.0
            }
        })

    # Then data is added into influxdb.
    engine.write_points(json_body, time_precision='ms')



def extract_candles():
    print('Getting list of BTC trade pairs...')
    #info = client.futures_exchange_info() #extract all the futures pairs
    info = client.get_exchange_info()
    logger.info("Found %d tickers of futures", len(info["symbols"]))
    for ticker in info["symbols"]:
        if str(ticker['symbol'])[-4:] == 'USDT':
            symbols.append(ticker['symbol'])


    # get 4h candles for symbols
    print(symbols)
    logger.info("Found %d USDT tickers", len(symbols))
    print('Loading candle data for symbols...')
    threads = []
    for sym in symbols:
        #print('BINANCE:' + sym)
        t = Thread(target=load_candles, args=(sym,))
        t.start()
        #print('BINANCE:' + sym)
        #load_candles(sym)
        #t = multiprocessing.Process(target=load_candles, args=(sym,))
        #threads.append(t)
        #t.start()
    print(candles)
    while len(candles) < len(symbols):
        print('%s/%s loaded' %(len(candles), len(symbols)), end='\r', flush=True)
        time.sleep(0.1)


def check_bounce_EMA(ticker):
    condition_one = ticker[1].get("high_price") > ticker[1].get("ema200") > ticker[1].get("low_price")
    condition_two = ticker[1].get("high_price") > ticker[1].get("ema89") > ticker[1].get("low_price")
    condition_three = ticker[1].get("stoch_k", 100) <= 25 and ticker[1].get("stoch_d", 100) <= 25
    return condition_three and (condition_one or condition_two)

def run_extract_candles():
    screened_list = []
    print(prices)
    for ticker in prices.items():
        if check_bounce_EMA(ticker):
            screened_list.append(ticker[0])
    store_pairs_on_influx(screened_list)

    print(screened_list)
    logger.info("Found Bounce tickers: {}".format(' '.join(map(str, screened_list))))
    logger.info("***************************************************************************************************************************************************************************")


def store_pairs_on_influx(screened_list):
    """
    Function to store the coins on influx make sure is runnig inlfud

    :param screened_list:
    :return:
    """
    client = InfluxDBClient(host='127.0.0.1', port=8086)
    client.switch_database('emabounce')
    json_body=[]

    for pairs in screened_list:
        coin_list = [{
            "measurement": "tickerbounce4h",
            "tags": {
                "unit": '200',
                "kind": 'EMA'

            },
            "fields": {
                "coin": pairs

            },
            "time": f'{datetime.utcnow().isoformat()}Z'
        }]
        try:
            if client.write_points(coin_list):
                success = True
            else:
                print("Error writing to InfluxDB")
        except:
            print("Error writing to InfluxDB")





def main():
    t1_start = perf_counter()
    job()
    calculate_ema()
    run_extract_candles()
    t1_stop = perf_counter()
    print("Elapsed time:", t1_stop, t1_start)
    print("Elapsed time during the whole program in seconds:",t1_stop - t1_start)

if __name__=="__main__":
    main()

