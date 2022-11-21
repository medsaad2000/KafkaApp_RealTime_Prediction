from aiokafka import AIOKafkaProducer
import asyncio
import json
import os
from random import randint

import csv
import time

#143.110.158.169 : Ip Address AWS
#Topic name : cloud666

# env variables
KAFKA_TOPIC = os.getenv('KAFKA_TOPIC','quickstart')
KAFKA_BOOTSTRAP_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')

# global variables
loop = asyncio.get_event_loop()


async def send_one():
    producer = AIOKafkaProducer(loop=loop, bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS)
    # get cluster layout and initial topic/partition leadership information
    await producer.start()
    try:
        # produce message
        with open('diabetes.csv', mode='r') as csv_file:
            csv_reader = csv.DictReader(csv_file)
            line_count = 0
            for row in csv_reader:
                if line_count == 0:
                    print(f'Column names are {", ".join(row)}')
                    line_count += 1
               
                print(f'Sending message with value: {row}')
                line_count += 1
                value_json = json.dumps(row).encode('utf-8')
                time.sleep(0.5)
                await producer.send_and_wait(KAFKA_TOPIC, value_json)
        # for i in range(10): 
        #     msg_id = f'{randint(1, 10000)}'
        #     value = {'message_id': msg_id, 'text': i, 'state': randint(1, 100)}
        #     print(f'Sending message with value: {value}------- {i}')
        #     value_json = json.dumps(value).encode('utf-8')
        #     await producer.send_and_wait(KAFKA_TOPIC, value_json)
    finally:
        # wait for all pending messages to be delivered or expire.
        await producer.stop()

# send message
loop.run_until_complete(send_one())