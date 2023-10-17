import os

from dotenv import load_dotenv
from fastapi import FastAPI

from .kafka import send
from .lib.models.alert import Alert

kafka_bootstrap_servers: str = os.environ.get("KAFKA_BOOTSTRAP_SERVERS")

app = FastAPI()
load_dotenv()

topic: str = os.environ.get("TOPIC")


# @app.on_event("startup")
# async def startup_event():
#     print("Starting up Hunters Incoming...")


@app.post("/")
async def process_alert(alert: Alert):
    print("DO")
    processed = False
    if alert.severity > 3:
        processed = True
        # send to kafka
        send(topic=topic, msg=alert.dict())

    return {"alert": alert, "processed": processed}
