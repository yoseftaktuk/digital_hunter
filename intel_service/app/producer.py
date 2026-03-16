from kafka import KafkaProducer
import json
import os
from log import log_event
kafka_uri = os.getenv('KAFKA_URI')


class KafkaService:
    def __init__(self):
        self.producer = KafkaProducer(bootstrap_servers=kafka_uri,
                                      max_request_size=31457280,
        value_serializer=lambda v: json.dumps(v, default=str).encode('utf-8')) 
          
    def on_send_success(self, record_metadata):
        print(record_metadata.topic)
        print(record_metadata.partition)
        print(record_metadata.offset)

    def on_sent_error(self, excp):
        log_event(level='error', message='I am an errback')    
    
    def send_to_kafka(self, topic: str,data: dict):
        self.producer.send(topic=topic, value=data).add_callback(self.on_send_success).add_errback(self.on_sent_error)
        log_event(level='info',message='send to kafka')
