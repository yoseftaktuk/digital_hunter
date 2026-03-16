from schema import Intel
from pydantic import ValidationError
from elastic_service import ElasticService
from producer import KafkaService
import json
kafka = KafkaService()
elastic = ElasticService()
class ValidService:
    def check_json(self, json_stringn_byts):#Checking that the json is valid
            try:
                data = json.loads(json_stringn_byts)
                return True, data
            except json.JSONDecodeError as e:
                data = {'data': json_stringn_byts}
                data['reason_error'] = f'Invalid json detected: {e}'
                print(f"Invalid json detected: {e}")
                return False, data
    def valid_data_not_missing(self, data: dict):#Checks if there is a lack of information or type errors
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


         