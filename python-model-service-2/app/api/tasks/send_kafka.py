"""
Author: Jason Eisele
Date: March 23, 2021 (My Birthday!)
Scope: Function to send message to Kafka
"""
import json
import os
import certifi

from confluent_kafka import Producer
from loguru import logger


def acked(err, msg):
    if err is not None:
        print(f"Failed to deliver message: {msg}: {err}")
    else:
        print(f"Message produced: {msg}")


def add_message_to_kafka(topic: str, key: str, value: dict):
    try:
        conf = {'bootstrap.servers': os.getenv("KAFKA_BROKERS"),
                'security.protocol': 'SASL_SSL',
                'sasl.username': os.getenv("KAFKA_KEY"),
                'sasl.password': os.getenv("KAFKA_SECRET"),
                'sasl.mechanism': 'PLAIN',
                'ssl.ca.location': certifi.where()
                }
        # Create Producer instance
        p = Producer(**conf)
        logger.debug("Connected to Kafka Broker")
    except Exception as e:
        logger.error(f"Could not connect to Kafka broker due to: {e}")
    try:
        p.produce(
            topic=os.getenv("TEST_ML_MODEL_PREDICT_TOPIC"),
            key=key,
            value=json.dumps(value),
            callback=acked
        )
        logger.debug(
            f"sent to topic: {os.getenv('TEST_ML_MODEL_PREDICT_TOPIC')}"
        )
        p.flush()
    except Exception as e:
        logger.error(f"Could not send message to Kafka due to: {e}")