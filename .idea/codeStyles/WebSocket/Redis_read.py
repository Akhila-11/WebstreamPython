#!/usr/bin/python
import redis
import pandas as pd
import numpy as np
from flask import Flask , render_template
from json import dumps

app = Flask(__name__)


@app.route("/")
def redisread():
    conn = redis.Redis('localhost')

    while True:
        redis_read=conn.hgetall("BitCoinTrans")
        redis_read=pd.DataFrame(redis_read.items())
        redis_read[0]=redis_read[0].str.decode(encoding='ASCII')
        redis_read[1]=redis_read[1].str.decode(encoding='ASCII')
        #print(redis_read)
        #Keep only last 100 transactions
        last_hundred= redis_read.tail(100)
        last_hundred = last_hundred.reset_index()
        last_hundred.columns = ['index','Timestamp','Counts']
        last_hundred=last_hundred.drop(columns=['index'])
        last_hundred.index =last_hundred.index + 1
        return render_template('LastHundred.html',tables=[last_hundred.to_html(classes='last_hundred')],
                                      titles = ['Last Hundred Transactions'])


def redisreadcall():

    app.debug = True
    app.run(port='5001')