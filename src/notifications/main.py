import asyncio
import os
import aiokafka
import logging
import os

from random import randint
from typing import Set, Any
from fastapi import FastAPI
from kafka import TopicPartition
from dotenv import load_dotenv


app = FastAPI()
load_dotenv()

# global variables
consumer_task = None
consumer = None

# env variables
ALERTS_TOPIC: str = os.getenv('ALERTS_TOPIC')
KAFKA_CONSUMER_GROUP_PREFIX = os.getenv('KAFKA_CONSUMER_GROUP_PREFIX', 'group')
KAFKA_BOOTSTRAP_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')

# initialize logger
logging.basicConfig(format='%(asctime)s - %(levelname)s - %(message)s',
                    level=logging.INFO)
log = logging.getLogger(__name__)


async def consume():
    global consumer_task
    global consumer

    consumer = aiokafka.AIOKafkaConsumer(
        ALERTS_TOPIC,
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
                log.info(f"NOTIFY: {msg.value}")
    except asyncio.CancelledError:
        log.info('Consumer task cancelled')
    finally:
        await consumer.stop()


async def initialize():
    global consumer_task
    consumer_task = asyncio.create_task(consume())


@app.on_event("startup")
async def startup_event():
    log.info('Initializing API ...')
    await initialize()


@app.on_event("shutdown")
async def shutdown_event():
    log.info('Shutting down API')
    consumer_task.cancel()
    await consumer.stop()
