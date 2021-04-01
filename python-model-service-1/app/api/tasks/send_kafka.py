"""
Author: Jason Eisele
Date: March 23, 2021 (My Birthday!)
Scope: Function to send message to Kafka
"""
import json
import os

from confluent_kafka import Producer
from loguru import logger


def acked(err, msg):
    if err is not None:
        print(f"Failed to deliver message: {msg}: {err}")
    else:
        print(f"Message produced: {msg}")


def add_message_to_kafka(topic: str, value: dict):
    try:
        """ Needed for connecting to local Kafka Broker on the Docker network"""
        # conf = {'bootstrap.servers': "localhost:9092"}
        conf = {'bootstrap.servers': "kafka:29092"}

        # Create Producer instance
        p = Producer(**conf)
        logger.debug("Connected to Kafka Broker")
    except Exception as e:
        logger.error(f"Could not connect to Kafka broker due to: {e}")
    try:
        p.produce(
            topic=topic,
            value=json.dumps(value),
            callback=acked
        )
        logger.debug(
            f"sent to topic: {topic}"
        )
        p.flush()
    except Exception as e:
        logger.error(f"Could not send message to Kafka due to: {e}")
