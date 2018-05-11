import numpy as np 
import pandas as pd 
from stockstats import StockDataFrame
pd.options.mode.chained_assignment = None

class TechnicalAnalyzer():
    def __init__(self, config):
        self.candles    = None
        self.candle     = None
        self.order_book = None
        self.ticker     = None
        self.config     = config
        # self.livePrice  = False

    def OHLC(self, data):
        self.candles = pd.DataFrame(data=data, columns=(['time', 'low', 'high', 'open', 'close', 'volume'])).iloc[::-1]
        # if self.livePrice and self.ticker and 'price' in self.ticker:
        #     self.candles.loc[0, 'close'] = float(self.ticker['price'])
        stock = StockDataFrame.retype(self.candles)
        if 'rsi_period' in self.config:
            self.candles['rsi'] = stock['rsi_{}'.format(self.config['rsi_period'])]
            self.candles['rsi_prior'] = self.candles['rsi'].shift(1)
        if 'cci_period' in self.config:
            self.candles['cci'] = stock['cci_{}'.format(self.config['cci_period'])]
            self.candles['cci_prior'] = self.candles['cci'].shift(1)
        if 'rolling_period' in self.config:
            self.candles['middle_band'] = self.candles['close'].rolling(self.config['rolling_period']).mean()
            self.candles['upper_band']  = self.candles['middle_band'] + self.candles['close'].rolling(self.config['rolling_period']).std() * self.config['upper_band_deviations']
            self.candles['lower_band']  = self.candles['middle_band'] - self.candles['close'].rolling(self.config['rolling_period']).std() * self.config['lower_band_deviations']

        self.candle = self.candles.iloc[-1].to_dict()


    def analyze(self, data=None):
        if 'ohlc' in data and data['ohlc']:
            self.OHLC(data['ohlc'])
        if 'ticker' in data and data['ticker']:
            self.ticker = data['ticker']
        if 'order_book' in data and data['order_book']:
            self.order_book   = data['order_book']

