import time, json, datetime
import pandas as pd
import numpy as np
from threading import Thread
#from multiprocessing import Pool
from multiprocessing.dummy import Pool as ThreadPool

class Bot():
    def __init__(self, api, strategy = None, strategies=[], sec_update=1):
        if strategy:
            strategies.append(strategy)
        self.strategies = strategies
        self.api        = api
        self.errors     = 1
        self.sec_update = sec_update
        self.running    = True
        self.ROBOT      = None

    def on_error(self, error):
        print("{}: {}".format(datetime.datetime.now(), error))
        if self.errors <= 100:
            self.errors += 1
        else:
            self.stop()

    def stop(self):
        if not self.running:
            print("Not running")
        else:
            print("Terminating Bot")
            self.running = False
            self.api.end()
            self.ROBOT.join(timeout=10)

    def start(self):
        def process():
            self.running = True
            print("Bot is running")
            while self.running: 
                try:
                    # pool = ThreadPool(np.min([3, len(self.strategies)]))
                    # pool.map(self.run, self.strategies)
                    # pool.close()
                    # pool.join()
                    for strategy in self.strategies:
                        self.run(strategy)
                except KeyboardInterrupt:
                    print("\n{}: Algorithm interupted manually".format(datetime.datetime.now()))
                    self.stop()
                except Exception as e:
                    self.on_error(e)
            else:
                print('Bot has stopped')
                
        self.ROBOT = Thread(target=process)
        self.ROBOT.start()
        
      
    def run(self, strategy):
        time.sleep(self.sec_update)
        # strategy will review the data and provide instructions
        strategy.speculate()

        # Cancel orders
        for order in [order for order in strategy.cancel_orders if order is not None]:
            self.api.cancel_order( order )

        time.sleep(0.1)

        # Place orders
        for order in [order for order in strategy.place_orders if order is not None]:
            response = self.api.place_order( **order )
            strategy.orders_placed_during_session.append(response["id"])