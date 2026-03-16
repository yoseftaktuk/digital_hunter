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
                   'signal_id': {'type': 'keyword'},
                   'entity_id': {'type': 'keyword'},
                   'reported_lat': {'type':'integer'},
                   'reported_lon':{'type':'integer'},
                   'signal_type': {'type': 'keyword'},
                   'priority_level': {'type':'integer'}
               }
               }}    
        return map
    
    def create_index(self, index_name):
        if not self.es.indices.exists(index=index_name):
            responnse = self.es.indices.create(index=index_name, body=self.mapping_intel())
            log_event(level='info', message='index create')
            return
        return
    
    def checks_if_exists(self, entity_id):
        query =  {
        'query': {
        'term': {
        'entity_id': entity_id
        }
        }
        }
        response = self.es.search(index='intel', body=query)
        if response['hits']['hits']:
                return True, response['hits']['hits']
        return False
        
    def upsert(self, data: dict, index_name):
        doc_id = data['signal_id']
        index_name = index_name
        document_body = data
        response = self.es.index(
            index=index_name,
            id=doc_id,
            document=document_body
        )
        print(response)
        return response
    
    def get_attak_from_elastic(self, entity_id):
        query = {
        'query': {
        'term': {
        'entity_id': entity_id
        }
        }
        }
        try:
            response = self.es.search(index='damage', body=query)
            if response['hits']['hits']:
                return True
        except elasticsearch.NotFoundError as e:
            return False
    
    



