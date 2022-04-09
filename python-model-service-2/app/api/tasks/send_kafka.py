"""
Author: Jason Eisele
Date: March 23, 2021 (My Birthday!)
Scope: Function to send message to Kafka
"""
import json

from confluent_kafka import Producer
from loguru import logger


def acked(err, msg):
    if err is not None:
        print(f"Failed to deliver message: {msg}: {err}")
    else:
        print(f"Message produced: {msg}")


def add_message_to_kafka(producer: Producer, topic: str, value: dict):
    try:
        producer.produce(
            topic=topic,
            value=json.dumps(value),
            callback=acked
        )
        logger.debug(
            f"sent to topic: {topic}"
        )
    except Exception as e:
        logger.error(f"Could not send message to Kafka due to: {e}")
