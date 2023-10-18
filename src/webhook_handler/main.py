import os
import json
import time
import asyncio

from dotenv import load_dotenv
from fastapi import FastAPI
from pydantic import BaseModel
from aiokafka import AIOKafkaProducer

app = FastAPI()
load_dotenv()

# env variables
KAFKA_TOPIC = os.getenv('TOPIC')
KAFKA_BOOTSTRAP_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')

# global variables
loop = asyncio.get_event_loop()
producer = AIOKafkaProducer(loop=loop, bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS)

class Alert(BaseModel):
    timestamp: int | None = time.time()
    severity: int | None = 1
    description: str



def on_delivery(err, msg):
    if err is not None:
        print(f"Delivery failed: {err}")
    else:
        print(f"Message delivered to {msg.topic()} [{msg.partition()}]")

async def send(topic: str, msg: Alert):
    try:
        await producer.send_and_wait(topic, msg)

    except Exception as err:
        print(f"Some Kafka error: {err}")


def kafka_serializer(value):
    return json.dumps(value).encode()


@app.on_event("startup")
async def startup_event():
    await producer.start()
    print("Starting up Webhook Handler...")
    

@app.on_event("shutdown")
async def startup_event():
    print("Stopping Webhook Handler...")
    await producer.stop()


@app.post("/")
async def process_alert(alert: Alert):
    print("Processing alert...")
    processed = False
    if alert.severity > 3:
        processed = True
        # send to kafka
        value_json = json.dumps(alert.dict()).encode('utf-8')
        await send(KAFKA_TOPIC, value_json)

    return {"alert": alert, "processed": processed}
