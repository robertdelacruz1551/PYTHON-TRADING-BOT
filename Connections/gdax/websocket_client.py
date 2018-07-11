# gdax/WebsocketClient.py
# original author: Daniel Paquin
# mongo "support" added by Drew Rice
#
#
# Template object to receive messages from the gdax Websocket Feed

from __future__ import print_function
import json
import base64
import hmac
import hashlib
import time
import socket
import errno
import datetime
from threading import Thread
import websocket
from websocket import create_connection, WebSocketConnectionClosedException
from pymongo import MongoClient 
from Connections.gdax.gdax_auth import get_auth_headers
from signal import signal, SIGPIPE, SIG_DFL

class WebsocketClient(object):
    def __init__(self, url="wss://ws-feed.gdax.com", products=None, message_type="subscribe", channels=None, should_print=False, auth=False, key=None, 
            b64secret=None, passphrase=None, mongo_collection=None, persist=False):
        signal(SIGPIPE,SIG_DFL) # this ignores the errno 32, broken pipe
        self.url = url
        self.products = products
        self.channels = channels
        self.type = message_type
        self.stop = False
        self.error = None
        self.ws = None
        self.thread = None
        self.auth = auth
        self.api_key = key
        self.api_secret = b64secret
        self.api_passphrase = passphrase
        self.should_print = should_print
        self.mongo_collection = mongo_collection
        self.persist = persist
        self.sub_params = None
        self.previouslyConnected = False
        self.msgPrintDueToError = False
        self.msgCount = 0
        if self.persist:
            self.data = []
        else:
            self.data = None

    def start(self):
        def _go():
            self._connect()
            self._listen()
            self._disconnect()

        self.thread = Thread(target=_go)
        self.thread.start()
    

    def _setup(self):    
        if self.stop:
            print("Websocket stopped")
            return

        if self.products is None:
            self.products = ["BTC-USD"]
        elif not isinstance(self.products, list):
            self.products = [self.products]
        # added
        if not isinstance(self.channels, list):
            self.channels = [self.channels]

        if self.url[-1] == "/":
            self.url = self.url[:-1]

        if self.channels is None:
            self.sub_params = {'type': 'subscribe', 'product_ids': self.products}
        else:
            self.sub_params = {'type': 'subscribe', 'product_ids': self.products, 'channels': self.channels}

        if self.auth:
            timestamp = str(time.time())
            message = timestamp + 'GET' + '/users/self/verify'
            message = message.encode('ascii')
            hmac_key = base64.b64decode(self.api_secret)
            signature = hmac.new(hmac_key, message, hashlib.sha256)
            signature_b64 = base64.b64encode(signature.digest()).decode('utf-8').rstrip('\n')
            self.sub_params['signature'] = signature_b64
            self.sub_params['key'] = self.api_key
            self.sub_params['passphrase'] = self.api_passphrase
            self.sub_params['timestamp'] = timestamp

    def _connect(self):
        try:
            if self.previouslyConnected:
                print("{}: Restarting connection to channel(s) {}".format(datetime.datetime.now(), ', '.join(self.channels)))

            self._setup()

            self.ws = create_connection(self.url)
            self.ws.send(json.dumps(self.sub_params))
            self.on_open()
        except Exception:
            print("{}: Failed to connect to channels {}... Trying again in 10 seconds...".format(datetime.datetime.now(), ', '.join(self.channels)))
            time.sleep(10)
            self._connect()
        else:
            if self.previouslyConnected:
                print("{}: Connected to channel(s) {}".format(datetime.datetime.now(), ', '.join(self.channels)))
            self.previouslyConnected = True


    def keepalive(self, interval=30):
        last_update_time = time.time()
        while not self.stop:
            try:
                time.sleep(1)
                current_time = time.time()
                if self.ws and (current_time - last_update_time) >= interval:
                    self.ws.ping("keepalive")
                    last_update_time = current_time
            except WebSocketConnectionClosedException as e:
                if self.stop:
                    break
                else:
                    self.on_error(e)
                    self._connect()
                    continue
            except IOError as e:
                if e.errno == errno.EPIPE:
                    continue
                else:
                    raise Exception(e)
            except Exception as e:
                print("{}: Error encountered while pinging the server: {}".format(datetime.datetime.now(), e))
                raise Exception(e)


    def _listen(self):
        keepalive = Thread(target=self.keepalive)
        keepalive.start()
        while not self.stop:
            try:
                data = self.ws.recv()
                if data:
                    msg = json.loads(data)
            except IOError as e:
                print("Error at IO")
                if e.errno == errno.EPIPE:
                    self._connect()
                    continue
                else:
                    self.on_error(e, data)
                    break
            except WebSocketConnectionClosedException as e:
                if self.stop:
                    break
                else:
                    self.on_error(e)
                    self._connect()
                    if 'ticker' not in self.channels:
                        self.msgPrintDueToError = True
                    continue
            except ValueError as e:
                print("Error at ValueError")
                self.on_error(e, data)
                break
            except Exception as e:
                print("Error at other")
                self.on_error(e, data)
                break
            else:
                self.on_message(msg)
        keepalive.join()

    def _disconnect(self):
        try:
            self.terminatingWs()
        except Exception as e:
            print("Error while disconnecting:\n     {}".format(e))
            pass
        else:
            self.on_close()

    def terminatingWs(self):
        if self.ws:
            if self.type == "heartbeat":
                self.ws.send(json.dumps({"type": "heartbeat", "on": False}))
            self.ws.abort()
            self.ws = None

    def close(self):
        try:
            print("Closing websocket connection to channel(s) {}".format(', '.join(self.channels)))
            self.stop = True
            self.terminatingWs()
            self.thread.join()
        except Exception as e:
            print("Error occured while attempting to close the connection: \n     {}".format(e))

    def on_open(self):
        if self.should_print:
            print("-- Subscribed! --\n")

    def on_close(self):
        if self.should_print:
            print("\n-- Socket Closed --")

    def on_message(self, msg):
        if msg['type'] != "subscriptions":
            if self.persist:
                self.data.append(msg)
            else:
                self.data = msg
        if self.should_print or self.msgPrintDueToError:
            if self.msgPrintDueToError:
                self.msgCount += 1
            if self.msgCount > 5:
                self.msgPrintDueToError = False
                self.msgCount = 0
            print(msg)
        if self.mongo_collection:  # dump JSON to given mongo collection
            self.mongo_collection.insert_one(self.data)

    def on_error(self, e, data=None):
        if not self.stop:
            self.error = e
            print('{}: Channel(s) {}: {} - data: {}'.format(datetime.datetime.now(), ', '.join(self.channels), e, data))
        

if __name__ == "__main__":
    import sys
    import gdax
    import time

    class MyWebsocketClient(gdax.WebsocketClient):
        def on_open(self):
            self.url = "wss://ws-feed.gdax.com/"
            self.products = ["BTC-USD", "ETH-USD"]
            self.message_count = 0
            print("Let's count the messages!")

        def on_message(self, msg):
            print(json.dumps(msg, indent=4, sort_keys=True))
            self.message_count += 1

        def on_close(self):
            print("-- Goodbye! --")


    wsClient = MyWebsocketClient()
    wsClient.start()
    print(wsClient.url, wsClient.products)
    try:
        while True:
            print("\nMessageCount =", "%i \n" % wsClient.message_count)
            time.sleep(1)
    except KeyboardInterrupt:
        wsClient.close()

    if wsClient.error:
        sys.exit(1)
    else:
        sys.exit(0)
