from pydantic import ValidationError
from producer import KafkaService
from schema import Attack
import json
kafka = KafkaService()

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
    def check_inject_attack_unknown_entity(self, data: dict):
        if 'TGT-UNKNOWN' in data['entity_id']:
              kafka.send_to_kafka(topic='dlq_signals_intel', data=data) 
              return True
        return False
    
    def valid_data_not_missing(self, data: dict):
        try:
            Attack(**data)
            return True
        except ValidationError as exc:
            data['reason_error'] = exc.errors()[0]['type']
            kafka.send_to_kafka(topic='dlq_signals_intel', data=data)
            print(repr(exc.errors()[0]['type']))
            return False
