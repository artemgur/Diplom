import json
from pprint import pprint

from kafka import KafkaConsumer

# To consume latest messages and auto-commit offsets
consumer = KafkaConsumer('postgres_source.public.my_table',
                         #group_id='my_group',
                         group_id=None,
                         #enable_auto_commit=False,
                         consumer_timeout_ms=1000,
                         #request_timeout_ms=15000,
                         auto_offset_reset='earliest',
                         bootstrap_servers=['kafka:9092'])
#consumer.poll(timeout_ms=2000)
for message in consumer:
    # message value and key are raw bytes -- decode if necessary!
    # e.g., for unicode: `message.value.decode('utf-8')`
    print("%s:%d:%d: key=%s value=%s" % (message.topic, message.partition,
                                        message.offset, message.key,
                                        message.value))

    #pprint(json.loads(message.value))
    #break
