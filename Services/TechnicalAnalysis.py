from sklearn.linear_model        import LinearRegression
from sklearn.model_selection     import train_test_split
from sklearn                     import preprocessing
from numpy.polynomial.polynomial import polyfit
import numpy as np 
import pandas as pd 
import time
from stockstats import StockDataFrame
pd.options.mode.chained_assignment = None



class TimeSeriesAnalysis():
    def __init__(self, ohlc, indicators):
        self.OHLC = ohlc
        self.ohlc = self.OHLC
        self.ind = indicators

    def analyze(self):
        self.ohlc = self.OHLC.copy()
        self.ohlc = StockDataFrame.retype( self.ohlc[['time','low','high','open','close','volume']] )
        self.ohlc[ self.ind ]
        return self.ohlc

    def find_groups(self, ohlc, smooth, delimiter):
        ohlc['smooth']      = ohlc['close'].rolling(smooth).mean()
        ohlc['delimiter']   = ohlc['close'].rolling(delimiter).mean()
        groups              = ohlc[ (ohlc['smooth']>=ohlc['delimiter']) & (ohlc['smooth'].shift(1)<ohlc['delimiter'].shift(1)) ].reset_index()[['index']]
        groups['next'] = groups['index'].shift(-1)
        for i,r in groups.iterrows():
            section  = ohlc.loc[ r['index']:r['next'] ]
            groups.loc[i, 'high']    = section['close'].max()
            groups.loc[i, 'low']     = section['close'].min()
            groups.loc[i, 'h index'] = section[ section['close'] == section['close'].max() ].index.min()
            groups.loc[i, 'l index'] = section[ section['close'] == section['close'].min() ].index.min()
        return groups

    def find_tops_and_bottoms(self, OHLC, smooth, delimiter):
        ohlc = OHLC.copy()
        ohlc.sort_index(inplace=True)
        groups  = self.find_groups(ohlc, smooth=smooth, delimiter=delimiter)
        support = pd.DataFrame(
                      groups[['h index','high']].values.tolist() + 
                      groups[['l index','low' ]].values.tolist(),
                      columns=['index','support']
                  ).set_index('index').sort_index()
        support['% change'] = support['support'] / support['support'].shift(1) - 1

        for i,r in support.iterrows():
            ohlc.loc[i:, 'last support'] = r['support']

        for i,r in support[::-1].iterrows():
            ohlc.loc[:i, 'next support'] = r['support']

        ohlc['% from last support'] = ohlc['close'].rolling(3).mean() / ohlc['last support'] - 1
        return ohlc


class Level2Analysis():
    def __init__(self, orderbook):
        self.BOOK = orderbook
        self.book = None

    def analyze(self):
        self.book             = self.BOOK.copy()
        self.book['strength'] = pd.qcut(self.book['size'].rank(method='first'), 10, labels=False)
        self.book['wall']     = 1.5 < abs(0.6745*(self.book['size'] - self.book['size'].median())) / self.book['size'].mad()
        
    def asks(self, remove_zeros=True):
        return self.book[ (self.book['side']=='asks') & (self.book['size'] > (0 if remove_zeros else -1)) ].sort_value('price',ascending=True )#.reset_index()#[['price','size','wall']]

    def bids(self, remove_zeros=True):
        return self.book[ (self.book['side']=='bids') & (self.book['size'] > (0 if remove_zeros else -1)) ].sort_value('price',ascending=False )#.reset_index()#[['price','size','wall']]

    def bids_next_wall(self):
        book = self.bids()
        return book[ book['wall'] ].iloc[1]
    
    def asks_next_wall(self):
        book = self.asks()
        return book[ book['wall'] ].iloc[1]
        
    def best_entry(self):
        book = self.bids(False)
        book = book.loc[ :book[ book['wall'] ].head(1).index.min() ]
        return book[ book['strength'] == book['strength'].min() ]['price'].max()

    def best_exit(self):
        book = self.asks(False)
        book = book.loc[ :book[ book['wall'] ].head(1).index.min() ]
        return book[ book['strength'] == book['strength'].min() ]['price'].min()


class SupportForecastingModel():
    def __init__(self, init_training_data, live_data=None):
        self.OHLC      = live_data
        self.model     = LinearRegression()
        self.features  = [ 'low','high','open','close','volume','smooth','delimiter','last support','% from last support']
        self.target    = [ 'next support' ]
        self.confidence= 0
        try:
            self.train(init_training_data)
            self.cdf   = pd.DataFrame(self.model.coef_[0], self.features, columns=['Coefficiant'])
        except:
            pass

    def find_groups(self, ohlc, smooth, delimiter):
        ohlc['smooth']      = ohlc['close'].rolling(smooth).mean()
        ohlc['delimiter']   = ohlc['close'].rolling(delimiter).mean()
        groups              = ohlc[ (ohlc['smooth']>=ohlc['delimiter']) & (ohlc['smooth'].shift(1)<ohlc['delimiter'].shift(1)) ].reset_index()[['index']]
        groups['next'] = groups['index'].shift(-1)
        for i,r in groups.iterrows():
            section  = ohlc.loc[ r['index']:r['next'] ]
            groups.loc[i, 'high']    = section['close'].max()
            groups.loc[i, 'low']     = section['close'].min()
            groups.loc[i, 'h index'] = section[ section['close'] == section['close'].max() ].index.min()
            groups.loc[i, 'l index'] = section[ section['close'] == section['close'].min() ].index.min()
        return groups
    
    def draw_trend_lines(self, ohlc, low=False, high=False, periods=2):
        self.find_tops_and_bottoms(ohlc)
        sides = []
        if low:  sides.append(['l index','low','bottoms'])
        if high: sides.append(['h index','high','tops'])
        for side in sides:
            x = self.groups.tail(2)[side[0]].values.tolist()
            y = self.groups.tail(2)[side[1]].values.tolist()
            b, m = polyfit(x, y, 1) # y = m * x + b

            for X,r in ohlc.loc[int(x[0]):].iterrows():
                ohlc.loc[X, side[2]] = m*X+b
    
    def find_tops_and_bottoms(self, OHLC):
        ohlc = OHLC.copy()
        ohlc.sort_index(inplace=True)
        self.groups  = self.find_groups(ohlc, 25, 50)
        self.support = pd.DataFrame(
                      self.groups[['h index','high']].values.tolist() + 
                      self.groups[['l index','low' ]].values.tolist(),
                      columns=['index','support']
                  ).set_index('index').sort_index()
        self.support['% change'] = self.support['support'] / self.support['support'].shift(1) - 1

        for i,r in self.support.iterrows():
            ohlc.loc[i:, 'last support'] = r['support']

        for i,r in self.support[::-1].iterrows():
            ohlc.loc[:i, 'next support'] = r['support']

        ohlc['% from last support'] = ohlc['close'].rolling(5).mean() / ohlc['last support'] - 1
        return ohlc
        
    def mean_negative_change(self):
        if self.support:
            return self.support[ self.support['% change']<0 ][ '% change' ].mean()
        else:
            raise Exception("Support levels have not been set. Run ta.find_tops_and_bottoms()")
    
    def mean_positive_change(self):
        if self.support:
            return self.support[ self.support['% change']>0 ][ '% change' ].mean()
        else:
            raise Exception("Support levels have not been set. Run ta.find_tops_and_bottoms()")
    
    def mean_distance_between_open_and_low(self, ohlc):
        return abs(ohlc[ ohlc['open'] > ohlc['low'] ]['open'] / ohlc[ ohlc['open'] > ohlc['low'] ]['low'] - 1).mean()

    def predict(self, OHLC=None):
        if OHLC:
            ohlc = OHLC
        else:
            ohlc = self.OHLC.copy()
        ohlc = self.prepare_ds(ohlc)
        return self.model.predict( ohlc[self.features].tail(1).values.tolist() )[0][0]

    def prepare_ds(self, ohlc):
        ohlc = self.find_tops_and_bottoms(ohlc)
        return ohlc.replace([np.inf, -np.inf], np.nan).dropna()
#         ohlc = preprocessing.scale(ohlc.values.tolist())
        
    def train(self, OHLC):
        ohlc = OHLC.copy()
        ohlc = self.prepare_ds(ohlc)
        X = ohlc[self.features]
        y = ohlc[self.target]
        X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.33, random_state=42)
        self.model.fit(X_train, y_train)
        self.confidence = self.model.score(X_test, y_test)
        self.cdf = pd.DataFrame(self.model.coef_[0], self.features, columns=['Coefficiant'])
        
    def test(self, ohlc):
        ohlc = self.prepare_ds(ohlc)
        for i, r in ohlc[self.features].iterrows():
            ohlc.loc[ i, 'prediction' ] = self.model.predict( [ r ] )[0]
        return ohlc

