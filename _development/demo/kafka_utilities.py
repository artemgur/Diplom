from kafka.admin import KafkaAdminClient
from kafka.errors import UnknownTopicOrPartitionError


def delete_topic(topic_name):
    admin_client = KafkaAdminClient(bootstrap_servers=['kafka:9092'])
    try:
        admin_client.delete_topics(topics=[topic_name])
        #print("Topic Deleted Successfully")
    except UnknownTopicOrPartitionError:
        print("Topic doesn't exist")