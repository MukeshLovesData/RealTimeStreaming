import kafka
from kafka import KafkaProducer
import time
import threading
import json
from quixstreams import Application
from quixstreams.kafka.configuration import ConnectionConfig
from services.producer.Config.Config import Settings
from services.producer.trade import Trade
from services.producer.kraken_websocket_api import KrakenWebSocketAPI




def get_properties(connection_string: str,
                   bootstrap_servers : str,
                   security_protocol: str,
                   sasl_mech:str,
                   sasl_user:str,
                   sasl_pass: str        ) -> dict:
 # Extract the namespace from the connection string
    start_index = connection_string.index('/') + 2
    end_index = connection_string.index('.')
    namespace = connection_string[start_index:end_index]
    
    # Configure the producer properties
    props = {
        'bootstrap.servers': bootstrap_servers,
        'security.protocol': security_protocol,
        'sasl.mechanism': sasl_mech,
        'sasl.username': sasl_user,
        'sasl.password': sasl_pass
    }
    
    return props




def publish_events(props:dict,topic:str,product_ids:list):
    """Publish messages to Kafka topic"""

    app = Application(
    broker_address=ConnectionConfig.from_librdkafka_dict(props, ignore_extras=True),  
    consumer_group="test",  
)
    
        # messages = {
        #         "id": "4", "name": "quixstream_4", "value": 1}

        
    with app.get_producer() as producer:
        kraken_api = KrakenWebSocketAPI(product_ids=product_ids)
        while not kraken_api.is_done():
            events: list[Trade] = kraken_api.get_trades()

            for event in events:
            #serialize an event
                topic_name = app.topic(name=topic, value_serializer='json')

                

                message = topic_name.serialize(key=event.product_ids, value=event.to_dict())
                producer.produce(topic=topic_name.name, 
                                    
                                        value=message.value )
                producer.flush()
                print(f"Sent message: {message.value}")
                

def main():
    # Get producer configuration
    required_config = Settings
    props = get_properties(bootstrap_servers=required_config.bootstrap_servers,
                           connection_string=required_config.connection_string,
                           security_protocol="SASL_SSL",
                           sasl_mech="PLAIN",
                           sasl_user="$ConnectionString",
                           sasl_pass=required_config.connection_string)
    
    # Create producer instance
    # producer = KafkaProducer(**props)
    
 
    publish_events(props=props, 
                topic=required_config.topic,
                product_ids = required_config.product_ids)


if __name__ == "__main__":
    main()

