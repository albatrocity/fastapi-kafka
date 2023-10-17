import asyncio
import os

from aiokafka import AIOKafkaConsumer
from dotenv import load_dotenv
from fastapi import FastAPI


app = FastAPI()
load_dotenv()
topic: str = os.environ.get("TOPIC")

loop = asyncio.get_event_loop()
kafka_bootstrap_servers: str = os.environ.get("KAFKA_BOOTSTRAP_SERVERS")


async def consume():
    consumer = AIOKafkaConsumer(
        topic,
        loop=loop,
        bootstrap_servers=kafka_bootstrap_servers,
    )

    try:
        await consumer.start()

    except Exception as e:
        print(e)
        return

    try:
        async for msg in consumer:
            print(f"Topic: {msg.topic} | Message: {msg.value}")

    finally:
        await consumer.stop()


@app.on_event("startup")
async def startup_event():
    print("Starting up Alerts...")


@app.get("/")
async def home():
    return {"message": "Hello!"}
