import json

from kafka import KafkaConsumer

from sources.base import Source


class DebeziumSource(Source):
    def __init__(self, name, kafka_topic_name, **kwargs):
        super().__init__(name)
        self._kafka_topic_name = kafka_topic_name
        self._kafka_configuration = kwargs


    # Make it async using aiokafka?
    def listen(self):
        consumer = KafkaConsumer(self._kafka_topic_name, **self._kafka_configuration)
        for message in consumer:
            print('b')
            # Empty messages appear on DELETE. These are tombstone messages, needed for Kafka and irrelevant in our case
            if message.value is None:
                continue

            message_dict = json.loads(message.value)
            old_row = message_dict['payload']['before']
            new_row = message_dict['payload']['after']
            # TODO properly read schema info and use it to properly parse values?
            self._update_materialized_views(old_row, new_row)