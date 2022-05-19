from enum import Enum
import json
import logging
import os

from elasticsearch import Elasticsearch
from elasticsearch.exceptions import ConnectionError
from kafka import KafkaConsumer

from udata_search_service.config import Config
from udata_search_service.domain.entities import Dataset, Organization, Reuse
from udata_search_service.infrastructure.utils import get_concat_title_org, log2p, mdstrip

ELASTIC_HOST = os.environ.get('ELASTIC_HOST', 'localhost')
ELASTIC_PORT = os.environ.get('ELASTIC_PORT', '9200')

KAFKA_HOST = os.environ.get('KAFKA_HOST', 'localhost')
KAFKA_PORT = os.environ.get('KAFKA_PORT', '9092')
KAFKA_API_VERSION = os.environ.get('KAFKA_API_VERSION', '2.5.0')

CONSUMER_LOGGING_LEVEL = int(os.environ.get("CONSUMER_LOGGING_LEVEL", logging.INFO))

# Topics and their corresponding indices have the same name
MODELS = [
    'dataset',
    'reuse',
    'organization',
]


class KafkaMessageType(Enum):
    INDEX = 'index'
    REINDEX = 'reindex'
    UNINDEX = 'unindex'


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

    topics = [f'{Config.UDATA_INSTANCE_NAME}.{model}.{message_type.value}'
              for model in MODELS
              for message_type in KafkaMessageType]
    consumer.subscribe(topics)
    logging.info('Kafka Consumer created')
    return consumer


class DatasetConsumer(Dataset):
    @classmethod
    def load_from_dict(cls, data):
        # Strip markdown
        data["description"] = mdstrip(data["description"])

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
        # Strip markdown
        data["description"] = mdstrip(data["description"])

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
        # Strip markdown
        data["description"] = mdstrip(data["description"])

        data["followers"] = log2p(data.get("followers", 0))
        data["views"] = log2p(data.get("views", 0))
        return super().load_from_dict(data)


def parse_message(val_utf8):
    try:
        message = json.loads(val_utf8)
        index_name = message["meta"].get("index")
        model, message_type = message["meta"]["message_type"].split('.')
        if model == 'dataset':
            dataclass_consumer = DatasetConsumer
        elif model == 'reuse':
            dataclass_consumer = ReuseConsumer
        elif model == 'organization':
            dataclass_consumer = OrganizationConsumer
        else:
            raise ValueError(f'Model Deserializer not implemented for model: {model}')

        if not message.get("data"):
            document = None
        else:
            document = dataclass_consumer.load_from_dict(message.get("data")).to_dict()
        return message_type, index_name, document
    except Exception as e:
        raise ValueError(f'Failed to deserialize message: {val_utf8}. Exception raised: {e}')


def consume_messages(consumer, es):
    logging.info('Ready to consume message')
    for message in consumer:
        value = message.value
        val_utf8 = value.decode('utf-8').replace('NaN', 'null')

        key = message.key.decode('utf-8')

        logging.debug(f'Message recieved with key: {key} and value: {value}')

        try:
            message_type, index_name, data = parse_message(val_utf8)
            index_name = f'{Config.UDATA_INSTANCE_NAME}-{index_name}'

            if message_type in [KafkaMessageType.INDEX.value,
                                KafkaMessageType.REINDEX.value]:
                es.index(index=index_name, id=key, document=data)

            elif message_type == KafkaMessageType.UNINDEX.value:
                if es.exists_source(index=index_name, id=key):
                    es.delete(index=index_name, id=key)

        except ValueError:
            logging.exception('ValueError when parsing message')
        except ConnectionError:
            logging.exception('ConnectionError with Elastic Client')
        except Exception:
            logging.exception('Exeption when indexing/unindexing')


def consume_kafka():
    logging.basicConfig(level=CONSUMER_LOGGING_LEVEL)

    es = create_elastic_client()
    consumer = create_kafka_consumer()
    consume_messages(consumer, es)
