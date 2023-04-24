import json
from abc import ABC, abstractmethod

from kafka import KafkaConsumer

from sources.base import Source


class KafkaSource(Source, ABC):
    def __init__(self, name, kafka_topic_name, **kwargs):
        super().__init__(name)
        self._kafka_topic_name = kafka_topic_name
        self._kafka_configuration = kwargs
        self._consumer = None

    @abstractmethod
    def _handle_kafka_message(self, message) -> (dict | None, dict | None):
        ...

    # Make it async using aiokafka?
    def listen(self):
        # TODO improve
        if self._consumer is None:
            self._consumer = KafkaConsumer(self._kafka_topic_name, **self._kafka_configuration)
        for message in self._consumer:
            # Empty messages appear on DELETE. These are tombstone messages, needed for Kafka and irrelevant in our case
            if message.value is None:
                continue

            old_row, new_row = self._handle_kafka_message(message.value)

            self._update_materialized_views(old_row, new_row)