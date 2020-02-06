#!/usr/bin/python
from time import sleep
from json import dumps
from  kafka import KafkaProducer
from  kafka import KafkaConsumer
import websocket
import json
import time
import pandas as pd


def Webstream():
    #Connect to websocket and subscribe to recieive transaction
    ws = websocket.create_connection("wss://ws.blockchain.info/inv")
    ws.send(json.dumps( {"op":"unconfirmed_sub"} ) )
    while True:
        response = ws.recv()
        response_dict = json.loads(response)
        response_dict_x = response_dict['x']
        response_dict_out  = response_dict_x['out']
        response_addr = [out_item["addr"] for out_item in response_dict_out]
        response_value = [out_item["value"] for out_item in response_dict_out]

        #Create a dictionary that holds address and value
        transaction_dict = dict(zip(response_addr, response_value))
        #print(transaction_dict)
        #print("Producing message to Kafka")
        producer = KafkaProducer(bootstrap_servers=['localhost:9092'],value_serializer=lambda x: dumps(x).encode('utf-8'),api_version=(0, 10, 1))

        #Produce messages to the created topic
        Produce_msg = producer.send("bitcoinstream",value=transaction_dict)
       # print(Produce_msg)

