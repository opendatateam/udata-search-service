import datetime
import json
import logging
import os

from elasticsearch import Elasticsearch, helpers
from elasticsearch.exceptions import ConnectionError
from kafka import KafkaConsumer

ELASTIC_HOST = os.environ.get('ELASTIC_HOST', 'localhost')
ELASTIC_PORT = os.environ.get('ELASTIC_PORT', '9200')

KAFKA_HOST = os.environ.get('KAFKA_HOST', 'localhost')
KAFKA_PORT = os.environ.get('KAFKA_PORT', '9092')
KAFKA_API_VERSION = os.environ.get('KAFKA_API_VERSION', '2.5.0')

LOGGING_LEVEL = int(os.environ.get("LOGGING_LEVEL", logging.INFO))

# Topics and their corresponding indices have the same name
TOPICS_AND_INDEX = [
    'dataset',
    'reuse',
    'organization',
]


logging.basicConfig(level=LOGGING_LEVEL)


def create_elastic_client():
    logging.info('Creating Elastic Client')
    es = Elasticsearch([{'host': ELASTIC_HOST, 'port': ELASTIC_PORT}])
    logging.info('Elastic Client created')
    return es


def create_kafka_consumer():
    logging.info('Creating Kafka Consumer')
    consumer = KafkaConsumer(
        bootstrap_servers=f'{KAFKA_HOST}:{KAFKA_PORT}',
        group_id='elastic',
        reconnect_backoff_max_ms=100000, # TODO: what value to set here?
        
        # API Version is needed in order to prevent api version guessing leading to an error
        # on startup if Kafka Broker isn't ready yet
        api_version=tuple([int(value) for value in KAFKA_API_VERSION.split('.')])
        )
    consumer.subscribe(TOPICS_AND_INDEX)
    logging.info('Kafka Consumer created')
    return consumer


class Deserializer():

    mask_keys = []

    def get_masked_keys(self, data):
        return {key: data.get(key) for key in self.mask_keys}

    def deserialize(self, data):
        raise NotImplementedError

class DatasetDeserializer(Deserializer):
    mask_keys = [
        'id',
        'title',
        'acronym',
        'url',
        'description',
        'resources_count',
        'temporal_coverage_start',
        'temporal_coverage_end'
    ]

    def deserialize(self, data):
        data = json.loads(data)
        result = self.get_masked_keys(data)
        result['created_at'] = datetime.datetime.fromisoformat(data['created_at'])
        result['organization_id'] = data['organization'].get('id') if data.get('organization') else None
        result['organization'] = data['organization'].get('name') if data.get('organization') else None
        result['orga_sp'] = data['organization'].get('public_service') if data.get('organization') else None
        result['orga_followers'] = data['organization'].get('followers') if data.get('organization') else None
        result['concat_title_org'] = data.get('description') + ' ' + str(result['organization'])
        result['dataset_reuses'] = data.get('reuses', 0)
        result['dataset_featured'] = data.get('featured', 0)
        result['dataset_views'] = data.get('views', 0)
        result['dataset_followers'] = data.get('followers', 0)
        result['spatial_granularity'] = data.get('granularity', 0)

        result['spatial_zones'] = 0 # TODO

        return result

class ReuseDeserializer(Deserializer):
    mask_keys = [
        'id',
        'title',
        'url',
        'description'
    ]

    def deserialize(self, data):
        data = json.loads(data)
        result = self.get_masked_keys(data)
        result['created_at'] = datetime.datetime.fromisoformat(data['created_at'])
        result['organization_id'] = data['organization'].get('id') if data.get('organization') else None
        result['organization'] = data['organization'].get('name') if data.get('organization') else None
        result['orga_followers'] = data['organization'].get('followers') if data.get('organization') else None
        result['reuse_datasets'] = data.get('datasets', 0)
        result['reuse_featured'] = data.get('featured', 0)
        result['reuse_views'] = data.get('views', 0)
        result['reuse_followers'] = data.get('followers', 0)

        return result

class OrganizationDeserializer(Deserializer):
    mask_keys = [
        'id',
        'name',
        'url',
        'description',
    ]

    def deserialize(self, data):
        data = json.loads(data)
        result = self.get_masked_keys(data)
        result['created_at'] = datetime.datetime.fromisoformat(data['created_at'])
        result['orga_sp'] = data.get('public_service')
        result['orga_followers'] = data.get('followers')
        result['orga_datasets'] = data.get('datasets', 0)

        return result


def consume_messages(consumer, es):
    logging.info('Ready to consume message')
    for message in consumer:
        value = message.value
        val_utf8 = value.decode('utf-8').replace('NaN','null')
        
        key = message.key
        index = message.topic

        logging.warning(f'Message recieved with key: {key} and value: {value}')

        if(val_utf8 != 'null'):
            if index == 'dataset':
                deserializer = DatasetDeserializer()
            elif index == 'reuse':
                deserializer = ReuseDeserializer()
            elif index == 'organization':
                deserializer = OrganizationDeserializer()
            else:
                logging.error(f'Model Deserializer not implemented for index: {index}')
                continue
            data = deserializer.deserialize(val_utf8)
            try:
                es.index(index=index, id=key.decode('utf-8'), document=data)
            except ConnectionError as e:
                logging.error(f'ConnectionError with Elastic Client: {e}')
                # TODO: add a retry mechanism?
        else:
                try:
                    if(es.exists_source(index=index, id=key.decode('utf-8'))):
                        es.delete(index=index, id=key.decode('utf-8'))
                except ConnectionError as e:
                    logging.error(f'ConnectionError with Elastic Client: {e}')


def main():
    es = create_elastic_client()
    consumer = create_kafka_consumer()
    consume_messages(consumer, es)

if __name__ == '__main__':
    main()
