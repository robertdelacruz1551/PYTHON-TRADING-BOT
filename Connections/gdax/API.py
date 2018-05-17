import Connections.gdax as GDAX
import pandas as pd
import numpy as np 
import math
import datetime
import time
import threading

class API():
    def __init__(self, passphrase, key, b64secret, symbol, increments=[], environment="development" ):
        self.passphrase  = passphrase
        self.key         = key
        self.b64secret   = b64secret
        self.symbol      = symbol
        self.ohlcTimeSync= False

        self.myordersColumns = ['client_oid', 'funds', 'limit_price', 'maker_order_id',
                                'maker_profile_id', 'maker_user_id', 'new_funds', 'old_funds',
                                'order_id', 'order_type', 'price', 'product_id', 'profile_id',
                                'reason', 'remaining_size', 'sequence', 'side', 'size',
                                'stop_price', 'stop_type', 'taker_fee_rate', 'taker_order_id',
                                'taker_profile_id', 'taker_user_id', 'time', 'trade_id', 'type',
                                'user_id','shares']

        if environment == "production":
            self.ws_url="wss://ws-feed.gdax.com"
            self.api_url="https://api.gdax.com"
        else:
            self.ws_url="wss://ws-feed-public.sandbox.gdax.com"
            self.api_url="https://api-public.sandbox.gdax.com"

        self.recorded       = []
        self.kill           = False
        self.public         = GDAX.PublicClient()
        self.private        = GDAX.AuthenticatedClient(url=self.api_url, key=self.key, b64secret=self.b64secret, passphrase=self.passphrase )
        self.tickerWS       = GDAX.WebsocketClient(    url="wss://ws-feed.gdax.com",  key=self.key, b64secret=self.b64secret, passphrase=self.passphrase, products=self.symbol, message_type="subscribe", channels="ticker", should_print=False, auth=False, persist=False)
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

        # {60, 300, 900, 3600, 21600, 86400}
        if '1min'  in self.increments: threading.Thread(name="OHLC 1min" ,target=self.getOHLC, kwargs={'increment':'1min', 'granularity':60}).start(); time.sleep(1)
        if '5min'  in self.increments: threading.Thread(name="OHLC 5min" ,target=self.getOHLC, kwargs={'increment':'5min', 'granularity':300}).start(); time.sleep(1)
        if '15min' in self.increments: threading.Thread(name="OHLC 15min",target=self.getOHLC, kwargs={'increment':'15min','granularity':900}).start(); time.sleep(1)
        if '1hour' in self.increments: threading.Thread(name="OHLC 1hour",target=self.getOHLC, kwargs={'increment':'1hour','granularity':3600}).start(); time.sleep(1)
        if '6hour' in self.increments: threading.Thread(name="OHLC 6hour",target=self.getOHLC, kwargs={'increment':'6hour','granularity':21600}).start(); time.sleep(1)
        if '1day'  in self.increments: threading.Thread(name="OHLC 1day" ,target=self.getOHLC, kwargs={'increment':'1day', 'granularity':86400}).start(); time.sleep(1)
    

    def getOHLC(self, increment, granularity):
        ohlcTime = None
        print("initializing {} ohlc".format(increment))
        while not self.kill:
            try:
                OHLC = self.public.get_product_historic_rates(product_id=self.symbol, granularity=granularity)
                if OHLC:
                    self.OHLC[increment] = OHLC
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
                print("{}: Error reported in {} OHLC: {}".format(datetime.datetime.now(), increment, e))
                time.sleep(1)
                continue
        print("OHLC {} thread stopped".format(increment))



    def getTicker(self):
        if self.tickerWS.data and self.tickerWS.data['type'] == 'ticker':
            return {
                "price": float(self.tickerWS.data['price']),
                "bid":   float(self.tickerWS.data['best_bid']),
                "ask":   float(self.tickerWS.data['best_ask'])
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
                candle = np.asarray(self.OHLC[increment][0], dtype=np.float)
                candle[1] = np.nanmin([ candle[1], ticker['price'] ]) # low
                candle[2] = np.nanmax([ candle[2], ticker['price'] ]) # high
                candle[4] = ticker['price'] # close
                if candle[3] == np.nan:
                   candle[3] = ticker['price'] # open
                self.OHLC[increment][0] = candle.tolist()   
                self.lastPrice = ticker['price']
            except Exception as e:
                print("price: {}, {} ohlc: {}".format(ticker['price'], increment, candle))
                raise Exception(e)
                
        ohlc = self.OHLC
        orders_placed = self.getOrders()

        return {
            "ticker": ticker,
            "orders_placed": orders_placed,
            "ohlc": ohlc
        }
    
    def cancelOrder(self, id):
        self.private.cancel_order(order_id=id)

    def placeOrder(self, type, side, size, price, trigger=np.nan):
        price = str(round(math.floor(price * 100) * 0.01, 2))
        size = str(round(math.floor(size * 100000000) * 0.00000001,8))
        if type == "stop":
            if side == "sell": stop = "loss"
            if side == "buy":  stop = "entry"
                
            payload = { "product_id": self.symbol, "size": size, "stop": stop }
            if math.isnan(trigger) == True:
                payload["type"] = "market"
                payload["stop_price"] = price
            else:
                trigger = str(round(math.floor(trigger * 100) * 0.01, 2))
                payload["type"] = "limit"
                payload["price"] = price
                payload["stop_price"] = trigger
        
        elif type == "limit":
            payload = { "product_id": self.symbol, "size": size, "price": price, "type": "limit", "stp": "co", "post_only": True }
        elif type == "market":
            payload = { "product_id": self.symbol, "size": size, "type": "market" }

        if side == "buy":
            return self.private.buy(**payload) 
        else:
            return self.private.sell(**payload)


    def end(self):
        self.tickerWS.close()
        self.ordersPlacedWS.close()
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
                orders_placed.loc[(orders_placed['side'] == "buy" )&((orders_placed['order_id'] == order['maker_order_id'])|(orders_placed['order_id'] == order['taker_order_id'])), 'funds']          -= (order['price'] * order['size']) - (order['price'] * order['size'] * order['taker_fee_rate'])
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