import pandas as pd
import numpy as np 
import math
import datetime

class AccountManager():
    def __init__(self, fund, risk, ordersColumns, fee=0.003, printBalance=False, reinvest=True):
        self.name         = None
        self.fund         = fund
        self.risk         = risk
        self.fee          = fee
        self.columns      = ordersColumns
        self.orders       = pd.DataFrame(data=[], columns=(self.columns))
        self.recorded     = []
        self.bookedOrdersByAlgo = []
        self.reinvest     = reinvest

        self.minOrderSize = 0.1
        self.riskAmount   = self.fund * self.risk

        self.fundsHold    = 0
        self.funds        = self.fund
        self.sharesHold   = 0
        self.shares       = 0
        self.profitAndLoss= 0

        self.openOrders   = pd.DataFrame(data=[], columns=(self.columns))
        self.printBalance = printBalance
        self.accountBalanceMessage   = None

    def lossCalc(self, purchased_at, size):
        invested = purchased_at * size
        fee = invested * self.fee
        risk = invested * self.risk
        stop = round(((fee + invested) - risk) / size, 2)
        return np.max([stop, 0.01])

    def size(self, buy):
        return round((self.funds / buy),8)


    def balance(self, orders):
        self.orders = orders[ ( ( orders['order_id'].isin(self.bookedOrdersByAlgo) ) | 
                                ( orders['maker_order_id'].isin(self.bookedOrdersByAlgo) ) | 
                                ( orders['taker_order_id'].isin(self.bookedOrdersByAlgo) ) ) ]
        # set account variables
        self.openOrders     = self.orders[ ( self.orders['type']!='filled' ) & ( self.orders['type']!='canceled' ) ]
        self.closedOrders   = self.orders[ ( self.orders['type']=='filled' ) | ( self.orders['type']=='canceled' ) ]
        self.openBuyOrders  = self.openOrders[self.openOrders['side'] == 'buy' ]
        self.openSellOrders = self.openOrders[self.openOrders['side'] == 'sell']
        self.openStopLoss   = self.openOrders[self.openOrders['order_type'] == 'loss']
        self.openStopEntry  = self.openOrders[self.openOrders['order_type'] == 'entry']
        
        # self.fundsHold      = np.max([ 0.0, round((self.openBuyOrders['price'] * self.openBuyOrders['remaining_size']).sum(),2)])
        self.fundsHold      = (self.openBuyOrders['price'] * self.openBuyOrders['remaining_size']).sum()
        self.funds         = np.max([ 0.0, round((self.fund + self.orders['funds'].sum() - self.fundsHold),2) ] )
        self.sharesHold     = np.max([ 0.0, (self.openSellOrders['remaining_size'].sum()) ] )
        self.shares         = np.max([ 0.0, (self.orders['shares'].sum() - self.sharesHold) ])

        # prints the account balance every time there's an update to the orders
        if int(self.funds):
            self.profitAndLoss = round(self.funds - self.fund,4)

        if not self.reinvest:
           self.funds = np.min([ self.funds, self.fund ])

        accountBalanceMessage = "P&L: {}, Funds available: {}, Funds on hold: {}, Shares available: {}, Shares on hold: {}".format(self.profitAndLoss, self.funds, self.fundsHold, self.shares, self.sharesHold)
        if self.accountBalanceMessage != accountBalanceMessage:
            if self.printBalance:
                print("{}: {} | {}".format(datetime.datetime.now(), self.name, accountBalanceMessage))
            self.accountBalanceMessage = accountBalanceMessage
