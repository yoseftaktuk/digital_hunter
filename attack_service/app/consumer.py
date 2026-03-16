from kafka import KafkaConsumer
from log import log_event
import os
from validation import ValidService
import time
from elastic_service import ElasticService
elastic = ElasticService()
elastic.create_index('attack')
valid = ValidService()
kafka_uri = os.getenv('KAFKA_URI') 
def get_from_kafka(topic: str):
    while True:
        try:
            consumer = KafkaConsumer(
                topic,
                group_id = '1',
                bootstrap_servers= kafka_uri,
                auto_offset_reset='earliest',
                enable_auto_commit=False
            )
            print('connect to kafka')
            log_event(level='info', message='connect to kafka')
            break
        except Exception:
            log_event(level='error', message='feild connect to kafka')
            print('no connect to kafka')
    while True:
        records = consumer.poll(timeout_ms=10000)
        time.sleep(0.1)
        for tp, messeges in records.items():
            for messeg in messeges:
                data = messeg.value
                if valid.check_json(json_stringn_byts=data):
                    data = valid.check_json(json_stringn_byts=data)[1]
                else: 
                    continue   
                if valid.valid_data_missing(data=data):
                    elastic.upsert(data=data, index_name='attack')
                    log_event(level='info',message=data)
        if records:
            try:
                consumer.commit_async()     
            except PythonFinalizationError:
                continue        
