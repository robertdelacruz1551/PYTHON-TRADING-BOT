import time
import numpy as np
import pandas as pd
import datetime
import math
import sys
import threading
import json

class Clerk():
    def __init__(self, api, strategy = None, strategies=[], daysRunning=1, archive=False, governByCandle='6hour'):
        if strategy:
            strategies.append(strategy)
        self.governByCandle = governByCandle
        self.strategies = strategies
        self.api        = api
        self.daysRunning= daysRunning
        self.endAt      = datetime.datetime.now() + datetime.timedelta(days=self.daysRunning)
        self.saveArchive= archive
        self.archive    = []
        self.errors     = []
        self.errorCount = 0
        print("Started at {} and will run for {} day(s)\n    End at {}".format(datetime.datetime.now(), self.daysRunning, self.endAt) )
        
        while datetime.datetime.now() < self.endAt: 
            try:
                self.run()
            except KeyboardInterrupt:
                print("\n{}: Algorithm interupted manually".format(datetime.datetime.now()))
                break
            except Exception as e:
                error = "{}: {} {}".format(datetime.datetime.now(), e, self.errorCount)
                print(error)
                if self.errorCount <= 100:
                    self.errorCount += 1
                    continue
                else:
                    print("Exceeded error count")
                    break
                    
        print('Ending program in 10 seconds.')
        self.api.end()
        time.sleep(10)
        print('Finished')
        sys.exit()


    def run(self):
        # the strategy will run return a list of instructions based on the calculation. The first set of instruction are a 
        # list of order cancelations. the second set of instructions are a list of orders to execute
        time.sleep(1)
        # get market data
        data = self.api.data()
        # if we have data then run the strategy
        if data:
            # # if the current trend is down then don't trade
            # # [ time, low, high, open, close, volume ]
            # if data['ohlc'][self.governByCandle] and data['ohlc'][self.governByCandle][0][3] < data['ohlc'][self.governByCandle][0][4]:
            #     time.sleep(5)
            # else:
            
            # Run the strategies
            for strategy in self.strategies:
                if len(data['ohlc'][strategy.increment]) == 0:# TODO:: this needs to move to the strategy. The clerk should not manage data governance
                    print(data['ohlc'][strategy.increment])
                    print("{}, is wating for {} ohlc data".format(strategy.name, strategy.increment))
                    break
                else:
                    # strategy will review the data and provide instructions
                    instructions    = strategy.speculate(data)
                    # Execute on the instructions
                    for cancel in instructions["cancelations"]:
                        self.api.cancelOrder( id=cancel )
                        time.sleep(1)

                    for order in instructions["orders"]:
                        orderPlaced = self.api.placeOrder( type=order["type"], side=order["side"], size=order["size"], price=order["price"], trigger=order["trigger"] )
                        if "message" not in orderPlaced and orderPlaced["status"] != "rejected":
                            strategy.account.bookedOrdersByAlgo.append(orderPlaced["id"])
                        else:
                            if 'message' in orderPlaced:
                                message = orderPlaced['message']
                            elif 'reject_reason' in orderPlaced:
                                message = orderPlaced['reject_reason']
                            else:
                                message = 'rejected'
                            print("{}: Strategy: {}, message: {}, order: [ type: {}, side: {} price: {}, size: {} ]".format(datetime.datetime.now(), strategy.name, message, order['type'], order['side'], order['price'], order['size']))

        else:
            print("{}: Waiting for data".format(datetime.datetime.now()))
            time.sleep(5)