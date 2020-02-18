#!/usr/bin/python
from time import sleep
from json import dumps
from  kafka import KafkaProducer
from  kafka import KafkaConsumer
import websocket
import json
import time
import pandas as pd
import numpy as np


def Webstream():
    #Connect to websocket and subscribe to recieive transaction
    ws = websocket.create_connection("wss://ws.blockchain.info/inv")
    ws.send(json.dumps( {"op":"unconfirmed_sub"} ) )
    transaction_df=pd.DataFrame([])
    while True:
        response = ws.recv()
        #print(type(response))
        response_dict = json.loads(response)
        response_dict_x = response_dict['x']
        response_dict_out  = response_dict_x['out']

        #Pull out Tags with data containing : Time , Address and Bitcoin Value
        response_dict_time = response_dict_x['time']
        response_addr = [out_item["addr"] for out_item in response_dict_out]
        response_value = [out_item["value"] for out_item in response_dict_out]

        #Create a df that holds address, time and value
        transaction_dict = {'addr':response_addr,'value':response_value}
        Total_rows =len(transaction_df)
        transaction_df = transaction_df.append(pd.DataFrame(transaction_dict),ignore_index=True)
        transaction_df['Row_num'] = np.arange(len(transaction_df))
        transaction_df.loc[transaction_df['Row_num'] >= Total_rows , 'Epoch_Timestamp'] = response_dict_time

        #Convert Epoch Timestamp to Datetime Format
        transaction_df.loc[transaction_df['Row_num'] >= Total_rows , 'Timestamp'] =pd.to_datetime(transaction_df['Epoch_Timestamp'],unit='s')
        #print(transaction_df)
        transaction_df_kafka = pd.DataFrame(transaction_df)
        transaction_df_kafka=transaction_df_kafka[['Row_num','addr', 'value','Timestamp']]

        transaction_df_kafka =transaction_df_kafka.to_json(date_format = 'iso')
        #print(transaction_df_kafka)
        #print("Producing message to Kafka")
        producer = KafkaProducer(bootstrap_servers=['localhost:9092'],value_serializer=lambda x: dumps(x).encode('utf-8'),api_version=(0, 10, 1))

        #Produce messages to the created topic
        Produce_msg = producer.send("bitcoinstreamnew",value=transaction_df_kafka)
        #print(Produce_msg)
