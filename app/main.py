from flask import Flask, request
from configparser import ConfigParser
from confluent_kafka import Producer
import logging
import json
from uuid import uuid4
import os


# Create the Kafka producer
p = Producer({
    'bootstrap.servers': 'broker:29092',
})

logger = logging.getLogger()

topic = "orders"

def produce_msg(msg):
    p.produce(topic, key=str(uuid4()), value=msg, on_delivery=callback)
    p.poll(10000)
    p.flush()

def callback(err, msg):
    if err:
        logger.error(err)
    else:
        logger.warning(f"An order for {msg.value().decode('utf-8')} has been made")

# Create flask app

app = Flask(__name__)

@app.post("/")
def index():
    data = request.json
    if not data.get("name") or not data.get("email") or not data.get("item"):
        return {"error": "Missing required fields"}
    produce_msg(json.dumps(data))
    return {"message": "Thank you for shopping with us!. You will receive an email with further instructions"}

@app.get("/")
def debug():
    produce_msg("Hello world")
    return {"app": "running"}

port = os.environ.get("PORT") or 5000
if __name__ == "__main__":
    app.run(debug=True, port=port, host="0.0.0.0")