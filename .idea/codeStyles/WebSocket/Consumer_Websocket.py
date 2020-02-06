#!/usr/bin/python
from time import sleep
from json import dumps
from  kafka import KafkaConsumer
from datetime import timedelta
import websocket
from  json import loads
import pandas as pd
import time
import  datetime
import numpy as np
import redis
import zlib


def consumer():
           #Connect to the Consumer to receive the subscribed transactions
           consumer = KafkaConsumer("bitcoinstream",bootstrap_servers=['localhost:9092'],value_deserializer=lambda x: loads(x.decode('utf-8')))
           df = pd.DataFrame([])
           df['Timestamp'] = ""
           Agg_trans = pd.DataFrame([])

           for msg in consumer:
                msg=msg.value
                today = datetime.datetime.today().replace(microsecond=0)
                three= today - datetime.timedelta(hours=3)
                Total_rows =len(df)
                df = df.append(pd.DataFrame(msg.items()),ignore_index=True)

                #Include Timestamp on newly consumed transactions
                df['Row_num'] = np.arange(len(df))
                df.loc[df['Row_num'] >= Total_rows , 'Timestamp'] = today

                #Rate of transaction on a given minute
                trans_per_min = df.groupby(pd.Grouper(key='Timestamp', freq='min'))['Row_num'].count()
                trans_per_min=trans_per_min.to_frame().reset_index()
                trans_per_min['Timestamp'] = trans_per_min['Timestamp'].astype(str)
                #trans_per_min[['Date','Time']]=trans_per_min.Timestamp.str.split(expand=True)
                #print(trans_per_min)
                trans_per_min_dict = dict(zip(trans_per_min['Timestamp'], trans_per_min['Row_num']))
                #print(trans_per_min_dict)


                #Send the data to redis
                print("Sending data to redis")
                conn = redis.Redis('localhost')
                conn.hmset("BitCoinTrans",trans_per_min_dict)


                #Persist only 3 hours data
                df.drop(df[df['Timestamp']< three ].index, inplace =True)
                #print(df)
                Agg_trans=pd.DataFrame(df.groupby(0,as_index=False)[1].sum())
                Agg_trans.columns = ['addr','Total_value']
                Agg_trans=Agg_trans.sort_values(by=['Total_value'],ascending=False)
                #print(Agg_trans)
                Agg_trans.to_csv(r'C:\Users\AKHILA K\Documents\Python Scripts\transactions.csv') #Give dst path


