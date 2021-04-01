"""
Author: Jason Eisele
Date: April 1, 2021
Scope: Main app controller for Kafka worker(s)
"""
from kafka_tasks import consume_message_from_kafka
from influx_tasks import insert_record, callback

if __name__ == "__main__":
    consume_message_from_kafka(topic="ml-ops", func=insert_record)
    # consume_message_from_kafka(topic="ml-ops", func=callback)
