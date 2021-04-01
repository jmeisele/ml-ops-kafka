"""
Author: Jason Eisele
Date: March 23, 2021
Scope: Function to consume message from Kafka
"""
import json
import os

from confluent_kafka import Consumer
from loguru import logger


def consume_message_from_kafka(topic: str, func: callable) -> None:
    try:
        """
        Basic configuration for connecting to a local Kafka Broker on the
        Docker network
        """
        conf = {
            # 'bootstrap.servers': "localhost:9092",
            'bootstrap.servers': "kafka:29092",
            'group.id': 'ml-ops-demo',
            'auto.offset.reset': 'earliest',
            'enable.auto.commit': 'true'
        }

        """
        Create Consumer instance
        'auto.offset.reset=earliest' to start reading from the beginning of
        the topic if no committed offsets exist
        """
        c = Consumer(**conf)
        logger.debug("Connected to Kafka Broker")

        # Subscribe to topic
        c.subscribe([topic])
        try:
            while True:
                msg = c.poll(1)
                if msg is None:
                    """
                    No message available within timeout.
                    Initial message consumption may take up to
                    `session.timeout.ms` for the consumer group to
                    rebalance and start consuming
                    """
                    logger.info("Waiting for message or event/error in poll()")
                    continue
                elif msg.error():
                    logger.error(f"error: {msg.error()}")
                else:
                    logger.info(msg.value())
                    func(body=msg.value())
        except KeyboardInterrupt:
            pass
        finally:
            # Leave group and commit final offsets
            c.close()
    except Exception as e:
        logger.error(e)
