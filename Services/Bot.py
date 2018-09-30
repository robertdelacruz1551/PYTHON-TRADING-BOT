import time
import pandas as pd
import datetime
from threading import Thread   
import json

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
            self.ROBOT.join()

    def start(self):
        def process():
            self.running = True
            print("Bot is running")
            while self.running: 
                try:
                    self.run()
                except KeyboardInterrupt:
                    print("\n{}: Algorithm interupted manually".format(datetime.datetime.now()))
                    self.stop()
                except Exception as e:
                    self.on_error(e)
            else:
                print('Bot has stopped working')
                
        self.ROBOT = Thread(target=process)
        self.ROBOT.start()
        

    def run(self):
        # the strategy will run return a list of instructions based on the calculation. The first set of instruction are a 
        # list of order cancelations. the second set of instructions are a list of orders to execute
        time.sleep(self.sec_update)

        # Run the strategies
        for strategy in self.strategies:
            # strategy will review the data and provide instructions
            strategy.speculate()

            # Cancel orders
            for order in strategy.cancel_orders:
                self.api.cancel_order( order )
            
            time.sleep(0.2)

            # Place orders
            for order in strategy.place_orders:
                response = self.api.place_order( **order )
                strategy.orders_placed_during_session.append(response["id"])
