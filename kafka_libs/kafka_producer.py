"""Kafka producer utilities."""

import json
import os
import time
from typing import Dict

from confluent_kafka import Message, Producer

config_file_path = f"{os.getcwd()}/client.properties"


def read_kafka_config(config_file: str) -> Dict:
    """
    Read Confluent Cloud configuration.

    :param config_file: config file path
    :return: configuration in a dictionary
    """
    conf = {}
    with open(config_file) as fh:
        for line in fh:
            line = line.strip()
            if len(line) != 0 and line[0] != "#":
                parameter, value = line.strip().split("=", 1)
                conf[parameter] = value.strip()

    return conf


def delivery_callback(err: str, msg: Message) -> None:
    """
    Delivery callback for Kafka producer.

    :param err: error message
    :param msg: message object
    :return: None
    """
    if err:
        print("ERROR: Message failed delivery: {}".format(err))
    else:
        print(
            "Produced event to topic {topic}: key = {key:12} value = {value:12}".format(
                topic=msg.topic(),
                key=msg.key().decode("utf-8"),
                value=msg.value().decode("utf-8"),
            )
        )


if __name__ == "__main__":
    producer = Producer(read_kafka_config(config_file=config_file_path))

    topic = "subscription_events"
    key = "key"
    messages = [
        {
            "customer_id": "tam_id",
            "event_type": "subscription_created",
            "order_id": "tam_order",
            "revenue": 100,
            "timestamp": int(time.time()),
        },
        {
            "event_type": "subscription_renewed",
            "order_id": "tam_order",
            "timestamp": int(time.time()),
        },
    ]

    for message in messages:
        producer.produce(
            topic=topic,
            key=key,
            value=json.dumps(message),
            callback=delivery_callback,
        )

    # Block until the messages are sent.
    producer.poll(10000)
    producer.flush()
