import kafka
from kafka import KafkaProducer
import time
import threading
import json
from quixstreams import Application
from quixstreams.kafka.configuration import ConnectionConfig
from services.consumer.config.config import Settings
from kafka import KafkaConsumer
import time

def main():
    # Connection string details
    connection_string = ""
    event_hub_name = ""
    consumer_group_name = ""
    
    # Get consumer properties
    props = get_properties(connection_string, consumer_group_name)
    
    # Create consumer and subscribe to topic
    consumer = KafkaConsumer(**props)
    consumer.subscribe([event_hub_name])
    
    # Consume events
    consume_events(consumer)

def get_properties(connection_string, consumer_group_name):
    # Extract namespace from connection string
    namespace = connection_string.split('//')[1].split('.')[0]
    bootstrap_servers = f"{namespace}.servicebus.windows.net:9093"
    
    props = {
        'bootstrap_servers': bootstrap_servers,
        'security_protocol': 'SASL_SSL',
        'sasl_mechanism': 'PLAIN',
        'group_id': consumer_group_name,
        'sasl_plain_username': '$ConnectionString',
        'sasl_plain_password': connection_string,
        'client_id': 'QuixstreamsExampleConsumer',
        'key_deserializer': lambda x: int.from_bytes(x, byteorder='big') if x else None,
        'value_deserializer': lambda x: x.decode('utf-8') if x else None,
        'auto_offset_reset': 'earliest'
    }
    
    return props

def consume_events(consumer):
    # Reset offset to beginning
    consumer.poll(0)
    
    # Get current assignments and seek to beginning
    partitions = consumer.assignment()
    for partition in partitions:
        consumer.seek_to_beginning(partition)
    
    # Continuously poll for new messages
    while True:
        records = consumer.poll(100)
        for partition, messages in records.items():
            for message in messages:
                print(f"Consumer Record: ({message.key}, {message.value}, {message.partition}, {message.offset})")
        
        consumer.commit()

if __name__ == "__main__":
    main()