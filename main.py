from typing import List
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from datetime import datetime
import json

from random import randint
from typing import Set, Any
from fastapi import FastAPI
from kafka import TopicPartition

import pickle
import uvicorn
import aiokafka
import asyncio
import json
import logging
import os

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

pickle_in = open("model_pkl","rb")
model=pickle.load(pickle_in)

# global variables
consumer_task = None
consumer = None
_state = 0

# env variables
KAFKA_TOPIC = os.getenv('KAFKA_TOPIC','quickstart')
KAFKA_CONSUMER_GROUP_PREFIX = os.getenv('KAFKA_CONSUMER_GROUP_PREFIX', 'group')
KAFKA_BOOTSTRAP_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')

# initialize logger
logging.basicConfig(format='%(asctime)s - %(levelname)s - %(message)s',
                    level=logging.INFO)
log = logging.getLogger(__name__)


@app.on_event("startup")
async def startup_event():
    log.info('Initializing API ...')
    await initialize()
    await consume()


@app.on_event("shutdown")
async def shutdown_event():
    log.info('Shutting down API')
    consumer_task.cancel()
    await consumer.stop()


@app.get("/")
async def root():
    return {"message": "Hello World"}


@app.get("/state")
async def state():
    return {"state": _state}



class ConnectionManager:

    def _init_(self) -> None:
        self.active_connections: List[WebSocket] = []

    async def connect(self, websocket: WebSocket):
        await websocket.accept()
        self.active_connections.append(websocket)

    def disconnect(self, websocket: WebSocket):
        self.active_connections.remove(websocket)

    async def send_personal_message(self, message: str, websocket: WebSocket):
        await websocket.send_text(message)

    async def broadcast(self, message: str):
        for connection in self.active_connections:
            await connection.send_text(message)


manager = ConnectionManager()

# define endpoint


@app.websocket("/")
async def websocket_endpoint(websocket: WebSocket):
    await manager.connect(websocket)
    try:
        async for msg in consumer:
            # x = json.loads(msg.value)
            log.info(f"Consumed msg: {msg}")
            log.info(f"Consumed msg: {msg.value.decode()}")
            prdiction= predict(msg.value.decode())
            log.info(f"prdiction -----: {prdiction}")
            message = {"prdiction":prdiction,"msg":msg.value.decode()}
            await manager.broadcast(json.dumps(prdiction))
            # _update_state(msg)
        # while True:
        #     # await manager.send_personal_message(f"You wrote: {data}", websocket)
       
        #     await manager.broadcast(json.dumps(message))
            
    except WebSocketDisconnect:
        manager.disconnect(websocket)
        message = {"time":current_time,"clientId":client_id,"message":"Offline"}
        await manager.broadcast(json.dumps(message))



async def initialize():
    loop = asyncio.get_event_loop()
    global consumer
    group_id = f'{KAFKA_CONSUMER_GROUP_PREFIX}-{randint(0, 10000)}'
    log.debug(f'Initializing KafkaConsumer for topic {KAFKA_TOPIC}, group_id {group_id}'
              f' and using bootstrap servers {KAFKA_BOOTSTRAP_SERVERS}')
    consumer = aiokafka.AIOKafkaConsumer(KAFKA_TOPIC, loop=loop,
                                         bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
                                         group_id=group_id)
    # get cluster layout and join group
    await consumer.start()

    partitions: Set[TopicPartition] = consumer.assignment()
    nr_partitions = len(partitions)
    if nr_partitions != 1:
        log.warning(f'Found {nr_partitions} partitions for topic {KAFKA_TOPIC}. Expecting '
                    f'only one, remaining partitions will be ignored!')
    for tp in partitions:

        # get the log_end_offset
        end_offset_dict = await consumer.end_offsets([tp])
        end_offset = end_offset_dict[tp]

        if end_offset == 0:
            log.warning(f'Topic ({KAFKA_TOPIC}) has no messages (log_end_offset: '
                        f'{end_offset}), skipping initialization ...')
            return

        log.debug(f'Found log_end_offset: {end_offset} seeking to {end_offset-1}')
        consumer.seek(tp, end_offset-1)
        msg = await consumer.getone()
        log.info(f'Initializing API with data from msg: {msg.value}')



        # update the API state
        _update_state(msg)
        return


async def consume():
    global consumer_task
    consumer_task = asyncio.create_task(send_consumer_message(consumer))


async def send_consumer_message(consumer):
    try:
        # consume messages
        async for msg in consumer:
            # x = json.loads(msg.value)
            # log.info(f"Consumed msg: {msg}")
            print('*****************')
            log.info(f"info de patient: {msg.value.decode()}")
            prdiction= predict(msg.value.decode())
            if prdiction == 1:
                log.info(f"prdiction -----: Positive")
            else :
                log.info(f"prdiction -----: Negative")
            print('------------------------------***------------------------------')
            # _update_state(msg)
    finally:
        # will leave consumer group; perform autocommit if enabled
        log.warning('Stopping consumer')
        await consumer.stop()


def _update_state(message: Any) -> None:
    value = json.loads(message.value)
    global _state
    _state = 1


def predict(data):
    data = json.loads(data)
    Pregnancies=int(data['Pregnancies'])
    Glucose=int(data['Glucose'])
    BloodPressure=int(data['BloodPressure'])
    SkinThickness=int(data['SkinThickness'])
    Insulin=int(data['Insulin'])
    BMI=float(data['BMI'])
    DiabetesPedigreeFunction=float(data['DiabetesPedigreeFunction'])
    Age=int(data['Age'])
    prediction = model.predict([[Pregnancies,Glucose,BloodPressure,SkinThickness,Insulin,BMI,DiabetesPedigreeFunction,Age]])[0]
    return int(prediction)


if __name__ == "_main_":
    uvicorn.run(app, host="localhost", port=8000)