import numpy as np 
import pandas as pd 
from stockstats import StockDataFrame
pd.options.mode.chained_assignment = None

class TechnicalAnalyzer():
    def __init__(self, indicators, increments):
        """https://pypi.org/project/stockstats/"""
        self.increments = increments
        self.indicators = indicators
        self.data = {
            "ticker": None,
            "order_book": {
                'bids': pd.DataFrame([], columns=['price','size','orders']),
                'asks': pd.DataFrame([], columns=['price','size','orders'])
            },
            "orders_placed": [],
            "ohlc": []
        }

    def ohlcAnalysis(self):
        for increment in self.increments:
            try:
                ohlc = StockDataFrame.retype( self.data['ohlc'][increment] )
                for indicator in self.indicators:
                    ohlc[indicator]
                self.data['ohlc'][increment] = ohlc.iloc[-1].to_dict()
            except Exception:
                print('Error while analyzing OHLC in TA')
                continue

    def orderBookAnalysis(self):
        for side in ['bids', 'asks']:
            try:
                self.data['order_book'][side]['wall'] = self.data['order_book'][side]['size'] > self.data['order_book'][side][self.data['order_book'][side]['size'] > self.data['order_book'][side]['size'].mean()]['size'].mean()
                self.data['order_book'][side]['strength'] = (
                    self.data['order_book'][side].iloc[:self.data['order_book'][side][self.data['order_book'][side]['wall']].index.min()+1]['size'].rank(method='dense') + 
                    self.data['order_book'][side].iloc[:self.data['order_book'][side][self.data['order_book'][side]['wall']].index.min()+1]['price'].rank(method='dense',ascending = self.data['order_book'][side].iloc[0]['price'] > self.data['order_book'][side].iloc[1]['price'])
                ).rank()
            except Exception:
                print('Error while analyzing the Order book in TA')
                continue

    def analyze(self, data=None):
        self.data = data

        self.ohlcAnalysis()
        self.orderBookAnalysis()