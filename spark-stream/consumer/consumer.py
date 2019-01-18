from kafka import KafkaConsumer
import kafka.errors
from hdfs import InsecureClient
import requests
import os
import sys
import time
import json

TOPIC = 'ethereum'
MAX_TRANSACTIONS = 1000
DIR = '/user/root/bitcoin'
PENDING_PATH = '/csv/pending.csv'


def connect_to_hdfs():
    return InsecureClient(os.environ['HDFS_HOST'], user='root')


def upload_to_hdfs(hdfs, content, format):
    filename = DIR + format[1:] + '/pending_' + \
               str(time.time()).split('.')[0]
    print("writing " + filename + " ...")
    hdfs.write(filename + format, data=content, encoding="utf-8")


def transform(tx):

    row = ','.join([tx["blockHash"], tx['from'], tx['to'], tx['value']])

    return row


def handle_msg(transactions, msg):
    transactions.append(json.loads(msg.value))

    if len(transactions) > MAX_TRANSACTIONS:
        content = "\n".join([transform(tx) for tx in transactions]) + "\n"
        pending_new_path = '/csv/pending_new.csv'
        hdfs.write(DIR + pending_new_path,
                   data=content, encoding='utf-8')

        concat_path = "http://namenode:50070/webhdfs/v1" + DIR + \
                      PENDING_PATH + "?op=CONCAT&sources=" + DIR + pending_new_path
        r = requests.post(concat_path)
        print(r.status_code, r.reason)

        # upload_to_hdfs(hdfs, json.dumps(transactions), ".json")

        transactions = []


def consume(hdfs, consumer):
    transactions = []

    try:
        hdfs.write(DIR + PENDING_PATH,
                   data="", encoding="utf-8")
    except:
        pass

    for msg in consumer:
        handle_msg(transactions, msg)


def connect_to_kafka():
    while True:
        try:
            consumer = KafkaConsumer(
                TOPIC, bootstrap_servers=os.environ['KAFKA_HOST'], group_id="hdfs-consumer")
            print("Connected to Kafka!")
            return consumer
        except kafka.errors.NoBrokersAvailable as e:
            print(e)
            time.sleep(3)


if __name__ == '__main__':
    time.sleep(60)

    hdfs = connect_to_hdfs()
    # hdfs.delete(BITCOIN_DIR, recursive=True)
    try:
        hdfs.makedirs(DIR)
    except:
        pass

    consumer = connect_to_kafka()
    consume(hdfs, consumer)
