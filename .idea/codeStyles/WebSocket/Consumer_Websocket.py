#!/usr/bin/python
from time import sleep
from json import dumps
import json
from  kafka import KafkaConsumer
from datetime import timedelta
import websocket
from  json import loads
import pandas as pd
import time
import  datetime
import numpy as np
import redis
from flask import Flask,render_template

app = Flask(__name__)
app.config['JSON_SORT_KEYS']= False

@app.route("/")
def consumer():
           #Connect to the Consumer to receive the subscribed transactions
           consumer = KafkaConsumer("bitcoinstreamnew",bootstrap_servers=['localhost:9092'],value_deserializer=lambda x: loads(x.decode('utf-8')))
           df = pd.DataFrame([])
           Agg_trans = pd.DataFrame([])

           #Read messages from Kafka as and when produced
           for msg in consumer:
                msg=msg.value
                today = datetime.datetime.utcnow().replace(microsecond=0)
                three= today - datetime.timedelta(hours=3)
                json_load =json.loads(msg)
                df = df.append(pd.DataFrame.from_dict(json_load))
                df['Timestamp'] = pd.to_datetime(df['Timestamp'])
                #print(df.dtypes)

                #Remove Time Zone
                df['Timestamp']=df['Timestamp'].dt.tz_localize(None)
                #print(df.dtypes)

                #Rate of transaction on a given minute
                trans_per_min = df.groupby(pd.Grouper(key='Timestamp', freq='min'))['Row_num'].count()
                #print(trans_per_min)
                trans_per_min=trans_per_min.to_frame().reset_index()
                #print(trans_per_min)
                trans_per_min['Timestamp'] = trans_per_min['Timestamp'].astype(str)
                trans_per_min_dict = dict(zip(trans_per_min['Timestamp'], trans_per_min['Row_num']))
                #print(trans_per_min_dict)


                #Send the data to redis
                #print("Sending data to redis")
                conn = redis.Redis('localhost')
                conn.hmset("BitCoinTrans",trans_per_min_dict)


                #Persist only 3 hours data
                df.drop(df[df['Timestamp']< three ].index, inplace =True)
                #print(df)
                Agg_trans=pd.DataFrame(df.groupby(df['addr'],as_index=False)['value'].sum())
                Agg_trans=Agg_trans.sort_values(['value'],ascending=False).reset_index()
                Agg_trans.columns = ['index','Address','Total_value']
                Agg_trans=Agg_trans.drop(columns=['index'])
                Agg_trans.index =Agg_trans.index + 1

                return render_template('AggTrans.html',tables=[Agg_trans.to_html(classes='Agg_trans')],
                                       titles = ['Aggregated transaction'])




def consumercall():

    app.debug = True
    app.run(port='5002')
