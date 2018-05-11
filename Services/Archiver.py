import datetime
import time

class Archive():
    def __init__(self, path, strategy=None):
        self.name = "{} - {}".format(strategy, datetime.datetime.now())
        self.path = path
        self.history = []
        self.archive = {
            'strategy': strategy,
            'block': None,
            'ohlc': None,
            'orders': []
        }

    def update(self, data):
        if self.archive['block'] != data['block']:
            self.history.append(self.archive)
            self.archive['block'] = data['block']
            self.archive['orders'] = []
            self.archive['cancelations'] = []

        self.archive['cancelations'].append(data['cancelations'])
        self.archive['orders'].append(data['orders'])
        self.archive['ohlc'] = data['ohlc']

    def save(self):
        print("Save history")
        pass