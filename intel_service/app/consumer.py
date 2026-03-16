from kafka import KafkaConsumer
from log import log_event
import os
from validation import ValidService
import time
from elastic_service import ElasticService
from haversine import haversine_km
elastic = ElasticService()
elastic.create_index('intel')
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
        records = consumer.poll(timeout_ms=1000)
        for tp, messeges in records.items():
            for messeg in messeges:
                data = messeg.value
                if valid.check_json(json_stringn_byts=data):
                    data = valid.check_json(json_stringn_byts=data)[1]
                else: 
                    continue  
                if valid.valid_data(data=data):
                    if elastic.check_if_exisst(data['signal_id']):
                        new_data = elastic.check_if_exisst(data['signal_id'])[1]
                        new_data['level_priority'] = 99
                        new_data['distance_between_targets'] = haversine_km(elastic.check_if_exisst(lat1=new_data['reported_lat']),
                                                         lon1=new_data['reported_lon'],
                                                           lat2=data['reported_lat'], 
                                                           lon2=data['reported_lon'])
                        elastic.upsert(data=data, index_name='intel')
                        log_event(level='info',message=data)
                        continue
                    data['distance_between_targets'] = 0
                    elastic.upsert(data=data, index_name='intel')
                    log_event(level='info',message=data)
                else:
                    continue    
        if records:
            try:
                consumer.commit_async()     
            except PythonFinalizationError:
                continue        
