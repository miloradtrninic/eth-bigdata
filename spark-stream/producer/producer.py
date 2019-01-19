from kafka import KafkaProducer
import kafka.errors
import asyncio
import os
import json
from web3 import Web3, WebsocketProvider
from hexbytes import HexBytes

try:
    import thread
except ImportError:
    import _thread as thread
import time

TOPIC = 'ethereum'

class HexJsonEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, HexBytes):
            return obj.hex()
        return super().default(obj)


while True:
    try:
        producer = KafkaProducer(bootstrap_servers=os.environ['KAFKA_HOST'])
        print("Connected to Kafka!")
        break
    except kafka.errors.NoBrokersAvailable as e:
        print(e)
        time.sleep(3)


def on_message(transaction, w3):
    tx = w3.eth.getTransaction(transaction)
    if tx is not None:
        tx_dict = dict(tx)
        tx_json = json.dumps(tx_dict, cls=HexJsonEncoder)
        if os.environ['LOG_LEVEL'] == 'debug':
            print(tx_json)
        producer.send(TOPIC, value=json.dumps(tx_json).encode('utf-8'))




async def log_loop(event_filter, poll_interval, w3):
    print("Pokrenuo loop")
    while True:
        for event in w3.eth.getFilterChanges(event_filter.filter_id):
            on_message(event, w3)
        await asyncio.sleep(poll_interval)


if __name__ == "__main__":

    w3 = Web3(WebsocketProvider("wss://rinkeby.infura.io/ws/v3/9b275464869943289e5cfc330547c4c9", websocket_kwargs={'timeout': 120}))
    print("Connected to RPC")
    print(w3.eth.blockNumber)
    web3_pending_filter = w3.eth.filter('pending')
    loop = asyncio.get_event_loop()
    try:
        loop.run_until_complete(
            asyncio.gather(
                log_loop(web3_pending_filter, 2, w3)))
    finally:
        loop.close()

