import os
import json
from typing import List

from aiokafka import AIOKafkaConsumer, AIOKafkaProducer

kafka_bootstrap_servers: str = os.environ.get("KAFKA_BOOTSTRAP_SERVERS")


async def send(topic: str, msg: List):
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
