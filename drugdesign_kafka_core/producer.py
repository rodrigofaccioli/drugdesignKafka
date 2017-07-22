from kafka import KafkaProducer
from kafka.errors import KafkaError
import json

def sentMsg2topic(topic_name, broker, key, value):


    # Create a producer instance serializing json messages format
    producer = KafkaProducer(bootstrap_servers=broker, value_serializer=lambda v: json.dumps(v).encode('utf-8'))

    # Asynchronous by default
    future = producer.send(topic_name, key=key.encode('UTF-8'), value=value)

    # Block for 'synchronous' sends
    try:
        record_metadata = future.get(timeout=10)
    except KafkaError:
        pass

    # Successful result returns assigned partition and offset
    ## print(record_metadata.topic)
    ## print(record_metadata.partition)
    ## print(record_metadata.offset)
    return record_metadata
