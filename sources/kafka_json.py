import json

from sources.kafka import KafkaSource


class KafkaJsonSource(KafkaSource):
    def _handle_kafka_message(self, message) -> (dict | None, dict | None):
        message_dict: dict = json.loads(message)

        method: str | None = message_dict.get('$method')
        if method is not None:
            method = method.lower()

        match method:
            case 'insert' | None:
                return None, message_dict
            case 'delete':
                del message_dict['$method']
                return message_dict, None
            case 'update':
                return message_dict['$old'], message_dict['$new']
