from kafka import KafkaProducer
import kafka.errors
import json
import websocket
from web3.auto import Web3
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
    print(tx['params']['result'])
    print(web3.eth.getTransaction(tx['params']['result']))
    #producer.send(TOPIC, value=json.dumps(tx[u'x']), key=str(tx[u'x'][u'hash']))
    # print(message)


if __name__ == "__main__":

    web3 = Web3(providers.WebsocketProvider("wss://mainnet.infura.io/_ws/"))
    web3_pending_filter = web3.eth.filter('pending')
    while True:
        transaction_list = web3_pending_filter.get_new_entries()
        transactions = [(web3.eth.getTransaction(h)) for h in transaction_list if h is not None]
        [print(tx) for tx in transactions if tx is not None]

    # ws = websocket.WebSocketApp(WS_SERVER,
    #                             on_message=on_message,
    #                             on_error=on_error,
    #                             on_close=on_close)
    # ws.on_open = on_open
    # ws.run_forever()
    # websocket.enableTrace(True)
