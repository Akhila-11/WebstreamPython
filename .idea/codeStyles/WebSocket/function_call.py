#!/usr/bin/python
import multiprocessing
from webstream import Webstream
from Consumer_Websocket import consumer
from Redis_read import readRedis

if __name__ == "__main__":
    #topic = input("Enter the topic created to stream data to Kafka")
    p1 = multiprocessing.Process(name='p1', target=(Webstream))
    p = multiprocessing.Process(name='p', target=consumer)
    p2 = multiprocessing.Process(name='p2', target=readRedis)
    p1.start()
    p.start()
    p2.start()


