import asyncio
import os
import json
import aiokafka
import logging
import os

from random import randint
from typing import Set, Any
from fastapi import FastAPI
from kafka import TopicPartition
from dotenv import load_dotenv
from pydantic import BaseModel


app = FastAPI()
load_dotenv()

# global variables
consumer_task = None
consumer = None
loop = asyncio.get_event_loop()
_state = 0

# env variables
KAFKA_TOPIC: str = os.getenv('CONSUME_TOPIC')
DOWNSTREAM_TOPIC: str = os.getenv('DOWNSTREAM_TOPIC')
KAFKA_CONSUMER_GROUP_PREFIX = os.getenv('KAFKA_CONSUMER_GROUP_PREFIX', 'group')
KAFKA_BOOTSTRAP_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')

# initialize logger
logging.basicConfig(format='%(asctime)s - %(levelname)s - %(message)s',
                    level=logging.INFO)
log = logging.getLogger(__name__)

class Notification(BaseModel):
    c_type: str
    priority: int
    message: str


async def initialize():
    global consumer_task
    consumer_task = asyncio.create_task(consume())


async def consume():
    global consumer_task
    global consumer

    consumer = aiokafka.AIOKafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        group_id=f"{KAFKA_CONSUMER_GROUP_PREFIX}-{randint(0, 1000)}",
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        auto_commit_interval_ms=1000,
        value_deserializer=lambda m: m.decode('utf-8')
    )

    await consumer.start()

    try:
        while True:
            async for msg in consumer:
                log.info(f"Received message: {msg.value}")
                await consume_message(msg)
    except asyncio.CancelledError:
        log.info('Consumer task cancelled')
    finally:
        await consumer.stop()

async def send_downstream(payload):
    log.info(f"Sending downstream: {payload}")
    # send to kafka
    producer = aiokafka.AIOKafkaProducer(loop=loop, bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS)
    try:
        await producer.start()
        await producer.send_and_wait(DOWNSTREAM_TOPIC, payload)
    except Exception as err:
        log.error(f"Some Kafka error: {err}")
    finally:
        await producer.stop()

async def consume_message(msg):
    # consume messages
    async for msg in consumer:
        global _state
        _state += 1
        log.info(f"Consumed msg: {msg}")
        try:
            payload = json.loads(msg.value)
        except Exception as err:
            log.error(f"Error parsing message: {err}")
            continue

        notification = {
            "c_type": "alert",
            "priority": payload['severity'],
            "message": payload['description']
        }
        value_json = json.dumps(notification).encode('utf-8')
        await send_downstream(value_json)

@app.on_event("startup")
async def startup_event():
    log.info('Initializing API ...')
    await initialize()


@app.on_event("shutdown")
async def shutdown_event():
    log.info('Shutting down API')
    consumer_task.cancel()
    await consumer.stop()


@app.get("/")
async def root():
    return {"count_alerts_processed": _state}