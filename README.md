Bitcoin Transaction read from web-socket

Prerequisites:

Create a Kafka Topic to stream the realtime transaction data from "wss://ws.blockchain.info/inv"
Python 3.7 version
Add your local paths to Consumer_websocket.py and Redis_read.py to store the reports as excel
Steps: 
1.function_call.py is the main python file that calls the other 3 functions:
a. webstream.py -- to read data from Websocket and stream to Kafka 
b. Consumer_websocket.py -- Read from Kafka topic and store 3 hours data to local; Send minute level transaction count to Redis 
c. Redis_read.py -- Consume from redis and find the last 100 transactions only and store in local 
2. I have induced the multiprocessing attribute of python to run the produce, consume and read from redis functionalities to occur in parallel, instead of running 3 python files in simultaneously 
3. I have considered the out tags attributes: addr and value to perform the transaction calculation
4. While consuming timestamp has been added to persist the last 3 hour data
5. While sending to redis one minute count has been calculatd and sent as a dict(Key - value pair) 
6. Consume only latest hundred transactions
