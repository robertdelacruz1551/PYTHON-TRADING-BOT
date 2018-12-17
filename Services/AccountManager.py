import pandas as pd
import numpy as np 
import math
import datetime

class AccountManager():
    def __init__(self, orders, products, product_info, orders_placed_during_session=[], risk=None, myholdings=None):
        self.product_info = product_info
        if type(products) != list:
            products = [ products ]
        self.currencies = list(set([pair.upper() for pair in '-'.join(products).split('-')]))
        holdings = myholdings if myholdings else { currency: float(input("Current {} holding? ".format(currency))) for currency in self.currencies}
        self.holdings = pd.DataFrame([holdings], columns=list(holdings.keys()))
        self.risk = (risk if risk else (float(input("Risk % of account? ")) * 0.01)) + 1
        self.ORDERS = orders
        self.orders = None
        self.balance = pd.DataFrame([], columns=['starting_balance','transactions','on_hold'])
        self.orders_placed_during_session = orders_placed_during_session
        self.update()
        
    def add_funds(self, currency, funds):
        currency = currency.upper()
        self.holdings.loc[0, currency ] += funds
        
    def remove_funds(self, currency, funds):
        self.holdings.loc[0, currency.upper()] -= funds
        
    def orders_placed(self):
        return self.orders.drop_duplicates(subset=['order_id'],keep='first')
    
    def find_order_by_id(self, id):
        return self.orders[ self.orders['order_id'] == id ].drop_duplicates(subset=['order_id'], keep='first')
    
    def update(self):
        self.orders   = self.ORDERS[ self.ORDERS['order_id'].isin(self.orders_placed_during_session) ]
        self.balance  = pd.DataFrame(self.holdings.sum().reset_index()).merge(
                            pd.DataFrame(self.orders[self.currencies].sum().reset_index()), 
                            how='left',
                            left_on='index',
                            right_on='index'
                        ).merge(
                            pd.DataFrame(self.orders[self.orders['order_type']=='limit'][['currency_on_hold','on_hold']].groupby(['currency_on_hold'])['on_hold'].sum()).reset_index(),
                            how='left',
                            left_on = 'index',
                            right_on = 'currency_on_hold'
                        ).merge(
                            pd.DataFrame(self.orders[self.orders['order_type']!='limit'][['currency_on_hold','on_hold']].groupby(['currency_on_hold'])['on_hold'].sum()).reset_index(),
                            how='left',
                            left_on = 'index',
                            right_on = 'currency_on_hold'
                        ).fillna(0).drop(['currency_on_hold_x','currency_on_hold_y'], axis=1)
        
        self.balance.columns=['currency','starting_balance','transactions','on_book','on_stop']
        self.balance['available'] = self.balance['starting_balance'] + self.balance['transactions'] - (self.balance['on_book'] + self.balance['on_stop'])
        self.balance.set_index('currency',inplace=True)

    def pair_split(self, product):
        return product.upper().split('-')

    def min_size(self, product):
        return self.product_info.loc[product.upper()]['base_min_size']

    def can_buy(self, product, price):
        base = self.pair_split(product)[1]
        size = self.balance.loc[base]['available'] / price
        return size if size >= self.min_size(product) else 0

    def can_sell(self, product, on=['available','on_stop'], percent=1):
        pair = self.pair_split(product)[0]
        size = self.balance.loc[pair][on].sum() * percent
        return size if size >= self.min_size(product) else 0

    def stop(self, purchase_at, risk=None):
        risk = risk if risk else self.risk
        return purchase_at / risk

    def pending_orders(self, side='buy,sell', order_type='limit,market,loss,entry', greater_than=0, lower_than=999999):
        return self.orders[ (self.orders['price']>greater_than)&(self.orders['price']<lower_than)&(self.orders['order_type'].isin(order_type.replace(' ','').lower().split(',')))&(self.orders['side'].isin(side.replace(' ','').lower().split(',')))&~(self.orders['status'].isin(['filled','canceled'])) ]

    def filled_orders(self, side='buy,sell', order_type='limit,market,loss,entry', greater_than=0, lower_than=999999):
        return self.orders[ (self.orders['price']>greater_than)&(self.orders['price']<lower_than)&(self.orders['order_type'].isin(order_type.replace(' ','').lower().split(',')))&(self.orders['side'].isin(side.replace(' ','').lower().split(',')))&(self.orders['status'].isin(['filled'])) ]