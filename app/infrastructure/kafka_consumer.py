import json
import logging
import os

from elasticsearch import Elasticsearch
from elasticsearch.exceptions import ConnectionError
from kafka import KafkaConsumer

from app.domain.entities import Dataset, Organization, Reuse
from app.infrastructure.utils import get_concat_title_org, log2p

ELASTIC_HOST = os.environ.get('ELASTIC_HOST', 'localhost')
ELASTIC_PORT = os.environ.get('ELASTIC_PORT', '9200')

KAFKA_HOST = os.environ.get('KAFKA_HOST', 'localhost')
KAFKA_PORT = os.environ.get('KAFKA_PORT', '9092')
KAFKA_API_VERSION = os.environ.get('KAFKA_API_VERSION', '2.5.0')

CONSUMER_LOGGING_LEVEL = int(os.environ.get("CONSUMER_LOGGING_LEVEL", logging.INFO))

# Topics and their corresponding indices have the same name
TOPICS_AND_INDEX = [
    'dataset',
    'reuse',
    'organization',
]


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
        reconnect_backoff_max_ms=100000,  # TODO: what value to set here?

        # API Version is needed in order to prevent api version guessing leading to an error
        # on startup if Kafka Broker isn't ready yet
        api_version=tuple([int(value) for value in KAFKA_API_VERSION.split('.')])
        )
    consumer.subscribe(TOPICS_AND_INDEX)
    logging.info('Kafka Consumer created')
    return consumer


class DatasetConsumer(Dataset):
    @classmethod
    def load_from_dict(cls, data):
        organization = data["organization"]
        data["organization"] = organization.get('id') if organization else None
        data["orga_followers"] = organization.get('followers') if organization else None
        data["orga_sp"] = organization.get('public_service') if organization else None
        data["organization_name"] = organization.get('name') if organization else None

        data["concat_title_org"] = get_concat_title_org(data["title"], data['acronym'], data['organization_name'])
        data["geozones"] = [zone.get("id") for zone in data.get("geozones", [])]

        # Normalize values
        data["views"] = log2p(data.get("views", 0))
        data["followers"] = log2p(data.get("followers", 0))
        data["reuses"] = log2p(data.get("reuses", 0))
        data["orga_followers"] = log2p(data.get("orga_followers", 0))
        data["orga_sp"] = 4 if data.get("orga_sp", 0) else 1
        data["featured"] = 4 if data.get("featured", 0) else 1

        return super().load_from_dict(data)


class ReuseConsumer(Reuse):
    @classmethod
    def load_from_dict(cls, data):
        organization = data["organization"]
        data["organization"] = organization.get('id') if organization else None
        data["orga_followers"] = organization.get('followers') if organization else None
        data["organization_name"] = organization.get('name') if organization else None

        # Normalize values
        data["views"] = log2p(data.get("views", 0))
        data["followers"] = log2p(data.get("followers", 0))
        data["orga_followers"] = log2p(data.get("orga_followers", 0))
        return super().load_from_dict(data)


class OrganizationConsumer(Organization):
    @classmethod
    def load_from_dict(cls, data):
        data["followers"] = log2p(data.get("followers", 0))
        return super().load_from_dict(data)


def parse_message(index, val_utf8):
    if index == 'dataset':
        dataclass_consumer = DatasetConsumer
    elif index == 'reuse':
        dataclass_consumer = ReuseConsumer
    elif index == 'organization':
        dataclass_consumer = OrganizationConsumer
    else:
        raise ValueError(f'Model Deserializer not implemented for index: {index}')
    try:
        data = dataclass_consumer.load_from_dict(json.loads(val_utf8)).to_dict()
        return data
    except Exception as e:
        raise ValueError(f'Failed to deserialize message: {val_utf8}. Exception raised: {e}')


def consume_messages(consumer, es):
    logging.info('Ready to consume message')
    for message in consumer:
        value = message.value
        val_utf8 = value.decode('utf-8').replace('NaN', 'null')

        key = message.key
        index = message.topic

        logging.info(f'Message recieved with key: {key} and value: {value}')

        if val_utf8 != 'null':
            try:
                data = parse_message(index, val_utf8)
                es.index(index=index, id=key.decode('utf-8'), document=data)
            except ValueError as e:
                logging.error(f'ValueError when parsing message: {e}')
            except ConnectionError as e:
                logging.error(f'ConnectionError with Elastic Client: {e}')
                # TODO: add a retry mechanism?
        else:
            try:
                if es.exists_source(index=index, id=key.decode('utf-8')):
                    es.delete(index=index, id=key.decode('utf-8'))
            except ConnectionError as e:
                logging.error(f'ConnectionError with Elastic Client: {e}')


def consume_kafka():
    logging.basicConfig(level=CONSUMER_LOGGING_LEVEL)

    es = create_elastic_client()
    consumer = create_kafka_consumer()
    consume_messages(consumer, es)
