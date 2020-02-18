#!/usr/bin/python
import multiprocessing
from webstream import Webstream
from Consumer_Websocket import consumercall
from Redis_read import redisreadcall


if __name__ == "__main__":
    print("URL for aggregated transactions for last 6 hours:  http://127.0.0.1:5001/ ")
    print("URL for latest 100 transaction from Redis:  http://127.0.0.1:5002/ ")
    p1 = multiprocessing.Process(name='p1', target=(Webstream))
    p = multiprocessing.Process(name='p', target=consumercall)
    p2 = multiprocessing.Process(name='p2', target=redisreadcall)
    p1.start()
    p.start()
    p2.start()


