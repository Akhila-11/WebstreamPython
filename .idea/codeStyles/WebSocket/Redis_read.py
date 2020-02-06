#!/usr/bin/python
import redis
import pandas as pd
import numpy as np

def readRedis():
    conn = redis.Redis('localhost')
    while True:
        redis_read=conn.hgetall("BitCoinTrans")
        redis_read=pd.DataFrame(redis_read.items())
        redis_read[0]=redis_read[0].str.decode(encoding='ASCII')
        redis_read[1]=redis_read[1].str.decode(encoding='ASCII')
        #print(redis_read)
        #Keep only last 100 transactions
        last_hundred= redis_read.tail(100)
        last_hundred['Row_num'] = np.arange(len(last_hundred))
        last_hundred.to_csv(r'C:\Users\AKHILA K\Documents\Python Scripts\lastHundredTransaction.csv') #Give dst path
        #print("Last 100 Transactions")
        print(last_hundred)
