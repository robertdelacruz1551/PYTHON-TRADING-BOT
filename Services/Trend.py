from sklearn.neighbors import KNeighborsClassifier
from sklearn.linear_model import LinearRegression
from sklearn.preprocessing import StandardScaler
from sklearn.metrics import classification_report,confusion_matrix
from sklearn.model_selection import train_test_split
from stockstats import StockDataFrame
import pandas as pd
import numpy as np
pd.options.mode.chained_assignment = None

class Forecast():
    """
    Trend forecasting model...
    
    This model will analyze historic OHLC data to classify future trend in price action between periods 
    of overbought and oversold. 
    
    Technical analysis - Th RSI indicator is added to the OHLC data to find periods between overbought and 
                         oversold. These periods are called blocks of trend. The blocks are classified with 
                         a signal value of 1, or -1. Each block is analyzed to determine the distance between 
                         each price period and the start of the block. Also, the price % change is calculated
                         for each price period. Other indicators are added and counted to determine the 
                         frequency of breaches within each block before change in trend. The KNN prediction 
                         model is fitted with calling self.predict will return the predicted trend signal for 
                         the next price action.
    
    @Variables:
           features: list of column names used to feed the model
           classification_report: a report of the fitting results
    
    @Param data: historic time, open, high, low, close, volume price action data.
    """
    def __init__(self, training_data):
        self.ohlc   = None
        self.train(training_data)
    
    def test(self, data):
        data = self.analyze(data)
        data['direction'] = data.apply(self.direction.predict ,axis=1)
        data['target']    = data.apply(lambda candle: candle['start price'] + (candle['start price'] * self.support.predict(candle)) ,axis=1)
        data['down']      = data[data['direction']== -1]['close']
        data['up']        = data[data['direction']==  1]['close']
        data[['close','down','up']].plot(figsize=(20,5), grid=True)#,'target','support'
        
    def analyze(self, data):
        return self.metrics(self.blocks(self.indicators(data)))
    
    def indicators(self, data):
        ohlc = StockDataFrame.retype( data[['time','low','high','open','close','volume']].copy() )
        ohlc['x'] = 1
        ohlc[['rsi_14','macd','rsv_14']]
        ohlc['vwap'] = ((ohlc['volume'] * ohlc['low']).cumsum() / ohlc['volume'].cumsum())
        ohlc['% change since open'] = ohlc[['open','low']].apply(lambda x: x.low / x.open - 1 ,axis=1)
        ohlc['hour of day'] = ohlc['time'].apply( lambda x: pd.to_datetime(x,unit='s').hour)
        ohlc['day of week'] = ohlc['time'].apply( lambda x: pd.to_datetime(x,unit='s').weekday())
        ohlc['range']                 = ohlc['rsi_14'].apply(lambda x: -1 if x<=30 else (1 if x>=70 else 0))
        # ohlc['range']                 = ohlc['rsv_14'].apply(lambda x: -1 if x<=20 else (1 if x>=80 else 0))
        ohlc['macd breach down']      = ((ohlc['macd'].shift(1)>=0) & (ohlc['macd']<=0)).astype(int)
        ohlc['macd breach up']        = ((ohlc['macd'].shift(1)<=0) & (ohlc['macd']>=0)).astype(int)
        ohlc['rsi breach oversold']   = ((ohlc['rsi_14'].shift(1)>=30) & (ohlc['rsi_14']<=30)).astype(int)
        ohlc['rsi breach overbought'] = ((ohlc['rsi_14'].shift(1)<=70) & (ohlc['rsi_14']>=70)).astype(int)
        ohlc['rsv breach oversold']   = ((ohlc['rsv_14'].shift(1)>=20) & (ohlc['rsv_14']<=20)).astype(int)
        ohlc['rsv breach overbought'] = ((ohlc['rsv_14'].shift(1)<=80) & (ohlc['rsv_14']>=80)).astype(int)
        return ohlc

    def blocks(self, ohlc):
        blocks = ohlc[(ohlc['range']!=0) | (ohlc.index.isin( ohlc.iloc[[0,-1]].index.tolist() ))]
        blocks = blocks[blocks['range']!=blocks['range'].shift(-1)]
        blocks['last time'] = blocks['time'].shift(1)
        blocks = blocks[['last time','time','range','x']].reset_index(drop=True).reset_index().merge(ohlc, 'inner', 'x').dropna()
        blocks = blocks[ (blocks['time_y']>=blocks['last time']) & (blocks['time_y']<=blocks['time_x']) ]
        return blocks

    def metrics(self, ohlc):
        ohlc = ohlc.groupby(['index','x']).agg({'close':'first','time_y':'first'}).reset_index().merge( ohlc[['index','time_y','open','high','low','close','volume','rsi_14','macd','hour of day','day of week','macd breach down','macd breach up','rsi breach oversold','rsi breach overbought','rsv breach oversold','rsv breach overbought','range_x','% change since open']], 'inner', 'index', suffixes=('_group','') )
        ohlc.columns = ['block','x','start price','start time','time','open','high','low','close','volume','rsi','macd','hour of day','day of week','macd breach down','macd breach up','rsi breach oversold','rsi breach overbought','rsv breach oversold','rsv breach overbought','signal','% change since open']
        blocks = ohlc[['start time','time','block']].reset_index().merge(ohlc, 'inner', 'block', suffixes=(' end',''))
        ohlc = blocks[ (blocks.time <= blocks['time end']) & (blocks.time >= blocks['start time']) ].groupby('index').agg({'close':'max','rsi breach oversold':'sum','rsi breach overbought':'sum','rsv breach oversold':'sum','rsv breach overbought':'sum','macd breach down':'sum','macd breach up':'sum','block':'count'}).merge(ohlc, 'inner', left_index=True, right_index=True, suffixes=(' count',''))
        ohlc['% change since block start'] = ohlc.apply(lambda x: (x['close'] / x['start price'])-1, axis=1).rolling(3).mean()
        ohlc['% short change'] = ohlc['close'].pct_change(14)
        ohlc['time passed'] = ohlc.apply(lambda x: x['time'] - x['start time'], axis=1)
        ohlc['next signal'] = ohlc['signal'].shift(-1)
        ohlc = ohlc.merge(ohlc.rename(index=str, columns={"% change since block start": "support"}).groupby('block')[['support']].last().shift(1).reset_index(), 'inner', 'block')
        return ohlc.drop_duplicates(subset='time',keep='first').fillna(0)[[ 'block','hour of day', 'day of week', 'start time', 'time', 'start price', 'block count', 'time passed', '% change since block start','% short change', 'open','high','low','close', 'volume', 
                                                                            'rsi breach oversold count', 'rsi breach overbought count', 'rsv breach oversold count','rsv breach overbought count', 'macd breach down count', 'macd breach up count',
                                                                            'rsi', 'macd', 'macd breach down', 'macd breach up', 'rsi breach oversold', 'rsi breach overbought', 'rsv breach oversold', 'rsv breach overbought', '% change since open','support', 'signal', 'next signal' ]]

    def support_set(self, processed_ohlc):
        training = processed_ohlc.groupby(['block','signal']).agg( 
            {'hour of day':'mean', 'day of week':'mean', 'block count':'max', 'time passed':'max', '% change since block start':'last', 
             'rsi breach oversold count':'max', 'rsi breach overbought count':'max','rsv breach oversold count':'max','rsv breach overbought count':'max', 'macd breach down count':'max', 'macd breach up count':'max',
             'rsi':'mean', 'macd':'mean', 'macd breach down':'max', 'macd breach up':'max', 'rsi breach oversold':'max','rsi breach overbought':'max', 'support':'mean' } ).reset_index()
        training['next signal'] = training['signal'].shift(-1)
        return training

    def train(self, ohlc):
        """
        This method trains the predictive model based on time series analysis of ohlc data
        
        @Param ohlc: A dataframe with time, open, high, low, close, volume (OHLC) data
        """
        metrics        = self.analyze(ohlc)
        support        = self.support_set(metrics)
        self.limits    = support.groupby('signal')[['block count','% change since block start','rsi breach oversold count','rsi breach overbought count','rsv breach oversold count','rsv breach overbought count','macd breach down count','macd breach up count']].mean()
        self.direction = Direction(metrics)
        self.support   = Support(support)
    
    def process(self, df):
        """ 
        This method returns the predicted directions of trend based on the knn classification model.

        Note! this method will select the last 500 records in the df to base the prediction on. This is to
        improve perfomance

        @Param df: A dataframe containing the time, open, high, low, close, and volume. This df will be 
                   processed, and the periods between overbought, and oversold will analyzed to find key 
                   indicators to create profiles to feed the predictive model.

        @Return:   returns the direction 1 = up, -1 = down, 0 = neutral/transition 
        """
        self.ohlc = self.analyze(df.tail(1000))
        self.ohlc['direction'] = self.ohlc.apply(self.direction.predict ,axis=1)
        self.ohlc['target']    = self.ohlc.apply(lambda candle: candle['start price'] + (candle['start price'] * self.support.predict(candle)) ,axis=1) #
        changed   = abs(self.ohlc['direction'].iloc[-6:-1].mean()) != 1
        direction = 0 if abs(self.ohlc['direction'].iloc[-4:-1].mean()) != 1 else self.ohlc['direction'].iloc[-3:-1].mean()
        return changed, direction

class Direction():
    def __init__(self, metrics):
        self.knn      = KNeighborsClassifier(n_neighbors=5, weights='distance')
        self.X        = []
        self.y        = []
        self.scaler   = StandardScaler()
        self.target   = 'signal'
        self.features = ['% change since block start','rsi breach oversold count', 'rsi breach overbought count',
                         'rsi breach oversold count','rsi breach overbought count',
                         'rsv breach oversold count','rsv breach overbought count',
                         'macd breach down count','macd breach up count','macd','support']
        self.train(metrics)
        
    def predict(self, candle):
        return self.knn.predict(self.scaler.transform([candle[self.features].values.tolist()]))[0]
    
    def report(self):
        X_train, X_test, y_train, y_test = train_test_split(self.X, self.y, test_size=0.1, random_state=42)
        self.knn.fit(X_train, y_train)
        pred = self.knn.predict(X_test)
        return classification_report(y_test, pred)
        
    def retrain(self, metrics, features=None, target=None):
        self.features = features if features else self.features
        self.target   = target if target else self.target
        self.train(metrics)
        
    def train(self, metrics):
        self.scaler.fit(metrics[self.features])
        self.X = self.scaler.transform(metrics[self.features].values.tolist())
        self.y = metrics[self.target].values.tolist()
        self.knn.fit(self.X, self.y)
        
class Support():
    def __init__(self, support_levels):
        self.lm       = LinearRegression(normalize=True)
        self.target   = 'next support'
        self.features = ['support']
        self.train(support_levels)
        
    def predict(self, candle):
        return self.lm.predict([candle[self.features].values.tolist()])[0]
    
    def train(self, support_levels):
        support_levels['next support'] = support_levels['support'].shift(-1)
        support_levels = support_levels.dropna()
        X = support_levels[self.features]
        y = support_levels[self.target]
        X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.1, random_state=42)
        self.lm.fit(X_train, y_train)
        self.cdf = pd.DataFrame(self.lm.coef_, X.columns, columns=['Coefficiant'])

