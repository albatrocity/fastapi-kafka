import os
import json

from dotenv import load_dotenv
from fastapi import FastAPI
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer

from .lib.models.alert import Alert

kafka_bootstrap_servers: str = os.environ.get("KAFKA_BOOTSTRAP_SERVERS")

app = FastAPI()
load_dotenv()

topic: str = os.environ.get("TOPIC")


async def send(topic: str, msg: Alert):
    print(f"Sending to Kafka: {topic} : {msg}")
    try:
        producer = AIOKafkaProducer(bootstrap_servers=kafka_bootstrap_servers)
        await producer.start()

        try:
            await producer.send_and_wait(topic, kafka_serializer(msg))
        finally:
            await producer.stop()

    except Exception as err:
        print(f"Some Kafka error: {err}")


def kafka_serializer(value):
    return json.dumps(value).encode()


@app.on_event("startup")
async def startup_event():
    print("Starting up Hunters Incoming...")


@app.post("/")
async def process_alert(alert: Alert):
    print("DO")
    processed = False
    if alert.severity > 3:
        processed = True
        # send to kafka
        send(topic=topic, msg=alert.dict())

    return {"alert": alert, "processed": processed}
