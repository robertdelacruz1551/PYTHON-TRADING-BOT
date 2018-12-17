import Connections.gdax as GDAX
import pandas as pd
import numpy as np 
import math
import datetime
import time
import threading
import sys
sys.path.insert(0, '/Users/Rob/Documents/Python Projects')
from CoinbaseProWebsocketClient.CoinbaseProWebsocketClient import CoinbaseWebsocket

class API():
    def __init__(self, passphrase, key, b64secret, symbol, increments=[], environment="development" ):
        print(sys.path)
        self.passphrase  = passphrase
        self.key         = key
        self.b64secret   = b64secret
        self.symbol      = symbol.upper()
        self.ohlcTimeSync= False

        if 'USD' in self.symbol:
            self.precision = 2
        else:
            self.precision = 5
        

        self.myordersColumns = ['client_oid', 'funds', 'limit_price', 'maker_order_id',
                                'maker_profile_id', 'maker_user_id', 'new_funds', 'old_funds',
                                'order_id', 'order_type', 'price', 'product_id', 'profile_id',
                                'reason', 'remaining_size', 'sequence', 'side', 'size',
                                'stop_price', 'stop_type', 'taker_fee_rate', 'taker_order_id',
                                'taker_profile_id', 'taker_user_id', 'time', 'trade_id', 'type',
                                'user_id','shares']

        if environment == "production":
            self.ws_url="wss://ws-feed.pro.coinbase.com"
            self.api_url="https://api.pro.coinbase.com"
        else:
            self.ws_url="wss://ws-feed-public.sandbox.pro.coinbase.com"
            self.api_url="https://api-public.sandbox.pro.coinbase.com"

        self.recorded       = []
        self.kill           = False
        self.public         = GDAX.PublicClient(api_url=self.api_url)
        self.private        = GDAX.AuthenticatedClient(url=self.api_url, key=self.key, b64secret=self.b64secret, passphrase=self.passphrase )
        
        self.ws             = CoinbaseWebsocket( 
                                products=[symbol],
                                channels=['user','ticker','level2'],
                                production=True,
                                credentials={
                                    'passphrase':  self.passphrase,
                                    'key': self.key,
                                    'b64secret': self.b64secret
                                })
        
        self.tickerWS       = GDAX.WebsocketClient(    url="wss://ws-feed.pro.coinbase.com",  key=self.key, b64secret=self.b64secret, passphrase=self.passphrase, products=self.symbol, message_type="subscribe", channels="ticker", should_print=False, auth=False, persist=False)
        self.ordersPlacedWS = GDAX.WebsocketClient(    url=self.ws_url,  key=self.key, b64secret=self.b64secret, passphrase=self.passphrase, products=self.symbol, message_type="subscribe", channels="user",   should_print=False, auth=True, persist=True)
        self.tickerWS.start()
        self.ordersPlacedWS.start()

        self.increments     = increments
        self.lastPrice      = 0
        self.OHLC = {
            '1min': [],
            '5min': [],
            '15min': [],
            '1hour': [],
            '6hour': [],
            '1day': []
        }
        self.order_book = {
            'bids': pd.DataFrame(data=[], columns=['price','size','orders'])[['size','price','orders']].apply(pd.to_numeric),
            'asks': pd.DataFrame(data=[], columns=['price','size','orders'])[['size','price','orders']].apply(pd.to_numeric)
        }

        print(self.increments)
        # {60, 300, 900, 3600, 21600, 86400}
        if '1min'  in self.increments: threading.Thread(name="OHLC 1min" ,target=self.getOHLC, kwargs={'increment':'1min', 'granularity':60}).start(); time.sleep(1)
        if '5min'  in self.increments: threading.Thread(name="OHLC 5min" ,target=self.getOHLC, kwargs={'increment':'5min', 'granularity':300}).start(); time.sleep(1)
        if '15min' in self.increments: threading.Thread(name="OHLC 15min",target=self.getOHLC, kwargs={'increment':'15min','granularity':900}).start(); time.sleep(1)
        if '1hour' in self.increments: threading.Thread(name="OHLC 1hour",target=self.getOHLC, kwargs={'increment':'1hour','granularity':3600}).start(); time.sleep(1)
        if '6hour' in self.increments: threading.Thread(name="OHLC 6hour",target=self.getOHLC, kwargs={'increment':'6hour','granularity':21600}).start(); time.sleep(1)
        if '1day'  in self.increments: threading.Thread(name="OHLC 1day" ,target=self.getOHLC, kwargs={'increment':'1day', 'granularity':86400}).start(); time.sleep(1)
    
        threading.Thread(name="Order book" ,target=self.getOrderBook ).start()
    

    def getOHLC(self, increment, granularity):
        ohlcTime = None
        print("initializing {} ohlc".format(increment))
        while not self.kill:
            try:
                OHLC = self.public.get_product_historic_rates(product_id=self.symbol, granularity=granularity)
                if OHLC:
                    self.OHLC[increment] = pd.DataFrame(data=OHLC, columns=(['time', 'low', 'high', 'open', 'close', 'volume'])).iloc[::-1]
                    self.lastPrice = 0
                    if ohlcTime and ohlcTime == OHLC[0][0]:
                        time.sleep(5)
                    else:
                        ohlcTime = OHLC[0][0]
                        interval = (granularity + (granularity - (int(time.time()) - OHLC[0][0])))
                        if interval > granularity:
                            interval -= granularity
                        while interval > 0 and not self.kill:
                            interval -= 1
                            time.sleep(1)
            except Exception as e:
                self.on_error('{} ohlc'.format(increment), e)
                continue
        print("OHLC {} thread stopped".format(increment))


    def getOrderBook(self):
        print("initializing order book")
        while not self.kill:
            try:
                order_book = self.public.get_product_order_book(product_id=self.symbol, level=2)
                self.order_book['bids'] = pd.DataFrame(data=order_book['bids'], columns=['price','size','orders'])[['size','price','orders']].apply(pd.to_numeric)
                self.order_book['asks'] = pd.DataFrame(data=order_book['asks'], columns=['price','size','orders'])[['size','price','orders']].apply(pd.to_numeric)
                time.sleep(1.1)
            except Exception as e:
                self.on_error('getOrderBook', e)
                continue
        print("Orderbook thread stopped")

    def on_error(self, origin, msg):
        print("{}: Error reported in {}: {}".format(datetime.datetime.now(), origin, msg))
        time.sleep(1)

    def getTicker(self):
        if self.tickerWS.data and self.tickerWS.data['type'] == 'ticker':
            return {
                # "time": self.tickerWS.data['time'] or time.time(),
                "product_id": self.tickerWS.data['product_id'],
                "price": float(self.tickerWS.data['price']),
                "bid":   float(self.tickerWS.data['best_bid']),
                "ask":   float(self.tickerWS.data['best_ask']),
                "24h_open":  float(self.tickerWS.data['open_24h']),
                "24h_high":  float(self.tickerWS.data['high_24h']),
                "24h_low":   float(self.tickerWS.data['low_24h']),
                "24h_percentage": (1 - (float(self.tickerWS.data['open_24h']) / float(self.tickerWS.data['price']))) * 100
            }
        else:
            return None

    def data(self):
        ticker = self.getTicker()

        for increment in self.increments:
            try:
                if self.lastPrice == ticker['price']:
                    break

                # [ time, low, high, open, close, volume ]
                self.OHLC[increment][['low', 'high', 'open', 'close']].fillna(ticker['price'])
                self.OHLC[increment][['volume']].fillna(0)
                self.OHLC[increment].loc[-1, 'low']   = np.nanmin([ self.OHLC[increment].iloc[-1]['low'], ticker['price'] ])
                self.OHLC[increment].loc[-1, 'high']  = np.nanmin([ self.OHLC[increment].iloc[-1]['high'],ticker['price'] ])
                self.OHLC[increment].loc[-1, 'close'] = ticker['price']
                if  pd.isnull(self.OHLC[increment].iloc[-1]['open']):
                    self.OHLC[increment].loc[-1, 'open'] = ticker['price']

            except Exception as e:
                print("price: {}, {} ohlc: {}".format(ticker['price'], increment, self.OHLC[increment].index[-1].values.tolist()))
                raise Exception(e)
                
        self.lastPrice = ticker['price']

        ohlc = self.OHLC
        orders_placed = self.getOrders()
        order_book = self.order_book
        return {
            "ticker": ticker,
            "order_book": order_book,
            "orders_placed": orders_placed,
            "ohlc": ohlc
        }
    
    def cancelOrder(self, id):
        self.private.cancel_order(order_id=id)

    def placeOrder(self, **kwargs):
        price = str(round(kwargs['price'], self.precision))
        size = str(round(math.floor(kwargs['size'] * 100000000) * 0.00000001,8))
        if kwargs['type'] == "stop":
            if kwargs['side'] == "sell": stop = "loss"
            if kwargs['side'] == "buy":  stop = "entry"
                
            payload = { "product_id": self.symbol, "size": size, "stop": stop }
            if 'trigger' in kwargs:
                trigger = str(round(kwargs['price'], 7)) #str(round(kwargs['trigger'], 6))
                payload["type"] = "limit"
                payload["price"] = price
                payload["stop_price"] = trigger
            else:
                payload["type"] = "market"
                payload["stop_price"] = price
        
        elif kwargs['type'] == "limit":
            if 'post_only' in kwargs: 
                post_only = kwargs['post_only']
            else:
                post_only = False
            payload = { "product_id": self.symbol, "size": size, "price": price, "type": "limit", "stp": "co", "post_only": post_only }
        elif kwargs['type'] == "market":
            payload = { "product_id": self.symbol, "size": size, "type": "market" }

        if kwargs['side'] == "buy":
            return self.private.buy(**payload) 
        else:
            return self.private.sell(**payload)


    def end(self):
        self.tickerWS.close()
        self.ordersPlacedWS.close()
        # self.ws.close()
        self.kill = True


    def getOrders(self):
        ordersList = self.ordersPlacedWS.data
        ordersList = pd.DataFrame(data=ordersList, columns=(self.myordersColumns))
        ordersList['sequence'] = "{}{}{}{}".format(ordersList['sequence'],ordersList['order_id'],ordersList['maker_order_id'],ordersList['taker_order_id'])
        #remove duplicates
        ordersList = ordersList.drop_duplicates(keep='first')
        ordersList['price'] = ordersList['price'].combine_first(ordersList['limit_price']).combine_first(ordersList['stop_price']).apply(pd.to_numeric)
        ordersList['order_type'] = ordersList['order_type'].combine_first(ordersList['stop_type'])
        ordersList['size']  = (ordersList['size']).apply(pd.to_numeric).fillna(0)
        ordersList['type']  = ordersList['reason'].combine_first(ordersList['type'])
        ordersList['remaining_size'] = ordersList['remaining_size'].apply(pd.to_numeric).fillna(0)
        ordersList['taker_fee_rate'] = ordersList['taker_fee_rate'].apply(pd.to_numeric).fillna(0)
        ordersList['funds'] = 0
        ordersList['shares']= 0 

        orders_placed = pd.DataFrame(data=[], columns=(self.myordersColumns))
        for index, order in ordersList.iterrows():
            # handle new orders
            if order['type'] in ['received','activate','open']:
                if order['order_id'] in orders_placed['order_id'].values.tolist():
                    orders_placed.loc[orders_placed['order_id'] == order['order_id'], 'type'] = order['type']
                else:
                    order['remaining_size'] = order['size'] 
                    orders_placed = orders_placed.append(order)

            # handle filled
            elif order['type'] in ['filled','canceled']:
                orders_placed.loc[orders_placed['order_id'] == order['order_id'], 'type'] = order['type']
                orders_placed.loc[orders_placed['order_id'] == order['order_id'], 'remaining_size'] = order['remaining_size']
            elif order['type'] == 'match':
                # buys
                orders_placed.loc[(orders_placed['side'] == "buy" )&((orders_placed['order_id'] == order['maker_order_id'])|(orders_placed['order_id'] == order['taker_order_id'])), 'funds']          -= (order['price'] * order['size']) + (order['price'] * order['size'] * order['taker_fee_rate'])
                orders_placed.loc[(orders_placed['side'] == "buy" )&((orders_placed['order_id'] == order['maker_order_id'])|(orders_placed['order_id'] == order['taker_order_id'])), 'shares']         += order['size']
                orders_placed.loc[(orders_placed['side'] == "buy" )&((orders_placed['order_id'] == order['maker_order_id'])|(orders_placed['order_id'] == order['taker_order_id'])), 'remaining_size'] =  order['remaining_size']
                # sells
                orders_placed.loc[(orders_placed['side'] == "sell")&((orders_placed['order_id'] == order['maker_order_id'])|(orders_placed['order_id'] == order['taker_order_id'])), 'funds']          += (order['price'] * order['size']) - (order['price'] * order['size'] * order['taker_fee_rate'])
                orders_placed.loc[(orders_placed['side'] == "sell")&((orders_placed['order_id'] == order['maker_order_id'])|(orders_placed['order_id'] == order['taker_order_id'])), 'shares']         -= order['size']
                orders_placed.loc[(orders_placed['side'] == "sell")&((orders_placed['order_id'] == order['maker_order_id'])|(orders_placed['order_id'] == order['taker_order_id'])), 'remaining_size'] -= order['size']

            # track the sequence number to ignore messages previously received
            if order['sequence'] in self.recorded:
                pass
            else:
                self.recorded.append(order['sequence'])
                
        return orders_placed