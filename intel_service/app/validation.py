from schema import Intel
from pydantic import ValidationError
from elastic_service import ElasticService
from producer import KafkaService
import json
kafka = KafkaService()
elastic = ElasticService()
class ValidService:
    def check_json(self, json_stringn_byts):
            try:
                data = json.loads(json_stringn_byts)
                return True, data
            except json.JSONDecodeError as e:
                data = {'data': json_stringn_byts}
                data['reason_error'] = f'Invalid json detected: {e}'
                print(f"Invalid json detected: {e}")
                return False, data
    def valid_data(self, data: dict):
        try:
            Intel(**data)
            if elastic.get_attak_from_elastic(data['entity_id']):
                data['reason_error'] = 'The target has already been destroyed.'
                kafka.send_to_kafka(topic='dlq_signals_intel', data=data)
                return False
            return True
        except ValidationError as exc:
            data['reason_error'] = exc.errors()[0]['type']
            kafka.send_to_kafka(topic='dlq_signals_intel', data=data)
            print(repr(exc.errors()[0]['type']))
            return False

    # def check_for_unknown_entity(self, data):   
    #     if 'TGT-UNKNOWN' in data['entity_id']:
    #           kafka.send_to_kafka(topic='dlq_signals_intel', data=data) 
    #           return True
    #     return False
    
         