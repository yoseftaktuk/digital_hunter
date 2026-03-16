from elasticsearch import Elasticsearch
import elasticsearch
import os
from log import log_event

class ElasticService:
    def __init__(self):
        try:
            self.es = Elasticsearch(os.getenv('ELASTIC_URI'))
        except Exception as e:
            print(e)

    def mapping_intel(self):
        map = {
            'mappings':{
               'properties':{
                   'timestamp': {'type': 'text'},
                   'attack_id': {'type': 'keyword'},
                   'entity_id': {'type': 'keyword'},
                   'result': {'type': 'keyword'}
               }
            }
        }
        return map
    
    def create_index(self, index_name):
        if not self.es.indices.exists(index=index_name):
            responnse = self.es.indices.create(index=index_name, body=self.mapping_intel())
            log_event(level='info', message='index create')
            return
        return
    

    def upsert(self, data: dict, index_name, doc_id):
        print(data)
        doc_id = doc_id
        index_name = index_name
        document_body = data
        response = self.es.index(
            index=index_name,
            id=doc_id,
            document=document_body
        )
        log_event(level='info', message='data update in damage index')
        return response
    

    def update_intel_db(self, entity_id, result: str): 
        query = {
        'query': {
        'term': {
        'entity_id': entity_id
        }
        }
        }
        try:
            response = self.es.search(index='intel', body=query)
            data = response['hits']['hits']
            if data:
                for item in data:
                    item['_source']['status'] = result
                    print(item)
                    self.upsert(data=item['_source'], index_name='intel', doc_id=item['_source']['signal_id'])
                    log_event(level='info', message='data update in intel index')
                return data
        except elasticsearch.NotFoundError as e:
            return False
