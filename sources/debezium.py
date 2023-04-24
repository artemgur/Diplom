import json

from sources.kafka import KafkaSource


class DebeziumSource(KafkaSource):
    def _handle_kafka_message(self, message) -> (dict | None, dict | None):
        message_dict = json.loads(message)
        old_row = message_dict['payload']['before']
        new_row = message_dict['payload']['after']
        # TODO properly read schema info and use it to properly parse values?
        return old_row, new_row
