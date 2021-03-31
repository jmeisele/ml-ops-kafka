"""
Author: Jason Eisele
Date: March 23, 2021 (My Birthday!)
Scope: Function to consume message from Kafka
"""
import json
import os
import certifi

from confluent_kafka import Consumer
from loguru import logger


def acked(err, msg):
    if err is not None:
        print(f"Failed to deliver message: {msg}: {err}")
    else:
        print(f"Message produced: {msg}")


def consume_message_from_kafka(topic: str, key: str):
    try:
        conf = {'bootstrap.servers': os.getenv("KAFKA_BROKERS"),
                'security.protocol': 'SASL_SSL',
                'sasl.username': os.getenv("KAFKA_KEY"),
                'sasl.password': os.getenv("KAFKA_SECRET"),
                'sasl.mechanisms': 'PLAIN',
                'ssl.ca.location': certifi.where(),
                'group.id': '',
                'auto.offset.reset': 'earliest',
                'enable.auto.commit': 'true'
        }
        # Create Consumer instance
        # 'auto.offset.reset=earliest' to start reading from the beginning of the
        #   topic if no committed offsets exist
        c = Consumer(**conf)
        # Subscribe to topic
        c.subscribe([topic])
        try:
            while True:
                msg = c.poll(1)
                if msg is None:
                    # No message available within timeout.
                    # Initial message consumption may take up to
                    # `session.timeout.ms` for the consumer group to
                    # rebalance and start consuming
                    print("Waiting for message or event/error in poll()")
                    continue
                elif msg.error():
                    print('error: {}'.format(msg.error()))
                else:
                    print(msg.value())
        except KeyboardInterrupt:
            pass
        finally:
            # Leave group and commit final offsets
            c.close()