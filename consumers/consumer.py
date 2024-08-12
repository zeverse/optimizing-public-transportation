"""Defines core consumer functionality"""

import logging

from confluent_kafka import Consumer, OFFSET_BEGINNING
from confluent_kafka.avro import AvroConsumer
from tornado import gen

from config import BROKER_PROPERTIES

logger = logging.getLogger(__name__)


class KafkaConsumer:
    """Defines the base kafka consumer class"""

    def __init__(
            self,
            topic_name_pattern,
            message_handler,
            is_avro=True,
            offset_earliest=False,
            sleep_secs=1.0,
            consume_timeout=0.1,
    ):
        """Creates a consumer object for asynchronous use"""
        self.topic_name_pattern = topic_name_pattern
        self.message_handler = message_handler
        self.sleep_secs = sleep_secs
        self.consume_timeout = consume_timeout
        self.offset_earliest = offset_earliest
        self.broker_properties = {
            **BROKER_PROPERTIES,
            "group.id": self.topic_name_pattern,
            "default.topic.config": {"auto.offset.reset": "earliest" if offset_earliest else "latest"}
        }

        # TODO: Create the Consumer, using the appropriate type.
        if is_avro is True:
            self.broker_properties["schema.registry.url"] = "http://localhost:8081"
            self.consumer = AvroConsumer(
                self.broker_properties
            )
        else:
            self.consumer = Consumer(self.broker_properties)
            pass

        self.consumer.subscribe(
            topics=list(topic_name_pattern),
            on_assign=self.on_assign,
        )

    def on_assign(self, consumer, partitions):
        """Callback for when topic assignment takes place"""
        # TODO: If the topic is configured to use `offset_earliest` set the partition offset to
        # the beginning or earliest
        if self.offset_earliest:
            for partition in partitions:
                partition.offset = OFFSET_BEGINNING
        logger.info("partitions assigned for %s", self.topic_name_pattern)
        consumer.assign(partitions)

    async def consume(self):
        """Asynchronously consumes data from kafka topic"""
        while True:
            num_results = 1
            while num_results > 0:
                num_results = self._consume()
            await gen.sleep(self.sleep_secs)

    def _consume(self) -> int:
        while True:
            msg = self.consumer.poll(timeout=self.consume_timeout)
            if msg is None:
                return 0
            else:
                if msg.error():
                    return 0
                else:
                    self.message_handler(msg)
                    return 1

    def close(self):
        self.consumer.close()
