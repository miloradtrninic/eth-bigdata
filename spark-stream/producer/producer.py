from kafka import KafkaProducer
import kafka.errors
import json
import websocket
from web3 import Web3
from web3 import providers
import os
import time
try:
    import thread
except ImportError:
    import _thread as thread
import time

WS_SERVER = 'wss://kovan.infura.io/ws'
SUB_MSG = '{"jsonrpc":"2.0", "id": 1, "method": "eth_subscribe", "params": ["newPendingTransactions"]}'
TOPIC = 'ethereum'

while True:
    try:
        #producer = KafkaProducer(bootstrap_servers=os.environ['KAFKA_HOST'])
        print("Connected to Kafka!")
        break
    except kafka.errors.NoBrokersAvailable as e:
        print(e)
        time.sleep(3)

def on_message(ws, message):
    tx = json.loads(message)
    print(auto.w3.eth.getTransaction(tx['params']['result']))
    #producer.send(TOPIC, value=json.dumps()


def on_error(ws, error):
    print(error)


def on_close(ws):
    print("### closed ###")


def on_open(ws):
    def run(*args):
        ws.send(SUB_MSG)
    thread.start_new_thread(run, ())


if __name__ == "__main__":
    web3 = Web3(providers.WebsocketProvider("wss://mainnet.infura.io/_ws/"))
    web3_pending_filter = web3.eth.filter('pending')

    while True:
        transaction_list = web3.eth.getFilterChanges(web3_pending_filter.filter_id)

        transactions = [web3.eth.getTransaction(h) for h in transaction_list]
        print(transactions)
