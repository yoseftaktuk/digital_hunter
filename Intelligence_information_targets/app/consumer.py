from kafka import KafkaConsumer
from log import log_event
import json
import os
kafka_uri = os.getenv('KAFKA_URI') 
def get_from_kafka(topic: str):
    while True:
        try:
            concumer = KafkaConsumer(
                topic,
                grup_id = "1",
                bootstrap_servers= kafka_uri,
                auto_offset_reset='earliest',
                enable_auto_commit=False,
                value_deserializer=lambda x: json.loads(x.decode('utf-8'))
            )
            log_event(level='info', message='connect to kafka')
            break
        except Exception:
            log_event(level='error', message='feild connect to kafka')
    while True:
        records = concumer.poll(timeout_ms=1000)
        for tp, messeges in records.items():
            for messeg in messeges:
                data = messeg.value
                print(data)
        if records:
            concumer.commit_async()        