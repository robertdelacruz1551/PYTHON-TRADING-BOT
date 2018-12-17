import Connections.gdax as GDAX
import pandas as pd
import numpy as np 
import math
import datetime
import time
from threading import Thread
import sys
from Connections.CoinbasePro.Websocket import Client

class API():
    def __init__(self, credentials, ticker=[], level2=[], ohlc=[]):
        self.environment = credentials.environment
        self.passphrase  = credentials.value['passphrase']
        self.key         = credentials.value['key']
        self.b64secret   = credentials.value['b64secret']
        
        # self.products    = [ product.upper() for product in products ]
        self.data        = None
        
        if self.environment  == "production":
            production   = True
            self.ws_url  ="wss://ws-feed.pro.coinbase.com"
            self.api_url ="https://api.pro.coinbase.com"
        else:
            production   = False
            self.ws_url  ="wss://ws-feed-public.sandbox.pro.coinbase.com"
            self.api_url ="https://api-public.sandbox.pro.coinbase.com"

        self.public      = GDAX.PublicClient(api_url=self.api_url)
        self.private     = GDAX.AuthenticatedClient(url=self.api_url, key=self.key, b64secret=self.b64secret, passphrase=self.passphrase )
        self.currencies  = pd.DataFrame(self.public.get_currencies()).set_index('id').apply(pd.to_numeric, **{'errors':'ignore'})
        self.product_info= pd.DataFrame(self.public.get_products()).set_index('id').apply(pd.to_numeric, **{'errors':'ignore'})

        self.ws          = Client( production=production,
                                   user=True,
                                   credentials={ 'passphrase':  self.passphrase, 'key': self.key, 'b64secret': self.b64secret },
                                   level2=[ product.upper() for product in level2 ],
                                   ticker=[ product.upper() for product in ticker ],
                                   ohlc=ohlc )
        self.ws.open()
        self.data        = self.ws.data 
        self.terminated  = self.ws.terminated

    def cancel_order(self, id):
        self.private.cancel_order(order_id=id)

    def end(self):
        self.ws.close()


    def place_order(self, **kwargs):
        base_increment = self.product_info.loc[kwargs['product_id']]['quote_increment']
        pair_min_size  = self.currencies.loc[kwargs['product_id'].split('-')[0]]['min_size']
        price = str(round(math.floor(kwargs['price'] / base_increment) * base_increment, 10))
        size  = str(round(math.floor(kwargs['size' ] / pair_min_size ) * pair_min_size, 10))
        if kwargs['type'] == "stop":
            if kwargs['side'] == "sell": stop = "loss"
            if kwargs['side'] == "buy":  stop = "entry"
                
            payload = { "product_id": kwargs['product_id'], "size": size, "stop": stop }
            if 'trigger' in kwargs:
                trigger = price #str(round(kwargs['trigger'], 6))
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
                post_only = True
            payload = { "product_id": kwargs['product_id'], "size": size, "price": price, "type": "limit", "stp": "co", "post_only": post_only }
        elif kwargs['type'] == "market":
            payload = { "product_id": kwargs['product_id'], "size": size, "type": "market" }

        if 'time_in_force' in kwargs and 'cancel_after' in kwargs:
            payload['time_in_force'] = kwargs['time_in_force']
            payload['cancel_after']  = kwargs['cancel_after']
            
        if kwargs['side'] == "buy":
            order = self.private.buy( **payload) 
        else:
            order = self.private.sell(**payload)

        if "message" in order or order["status"] == "rejected":
            if 'message' in order:
                message = order['message']
            elif 'reject_reason' in order:
                message = order['reject_reason']
            else:
                message = 'rejected*'
            raise Exception("Failed to place order {} \n \t{}".format(message, payload))
        else:
            return order

