from schema import Damage
from pydantic import ValidationError
from elastic_service import ElasticService
from producer import KafkaService
import json
from log import log_event
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
                log_event(level='error', message=f'invalid json detected {e}')
                return False, data
    def valid_data_not_missing(self, data: dict):#Checks if there is a lack of information or type errors
        try:
            Damage(**data)
            return True
        except ValidationError as exc:
            data['reason_error'] = exc.errors()[0]['type']
            kafka.send_to_kafka(topic='dlq_signals_intel', data=data)
            log_event(level='error', message= 'missing data')
            return False
