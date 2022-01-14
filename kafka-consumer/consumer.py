import dataclasses
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

@dataclasses.dataclass
class Base():

    @classmethod
    def load_fields(cls, raw_data):
        data = json.loads(raw_data)
        fields = [f.name for f in dataclasses.fields(cls)]
        return {key: data[key] for key in data if key in fields}

    def to_dict(self):
        return dataclasses.asdict(self)


@dataclasses.dataclass
class Dataset(Base):
    id: str
    title: str
    acronym: str
    url: str
    created_at: datetime
    views: int
    followers: int
    reuses: int
    featured: int
    resources_count: int
    description: str    
    organization: str

    geozones: str = None
    granularity: str = None
    temporal_coverage_start: datetime = None
    temporal_coverage_end: datetime = None

    orga_sp: int = 0
    orga_followers: int = 0
    organization_id: str = None

    concat_title_org: str = None

    def __post_init__(self):
        # We need to get value from nested organization dict and then set it to str
        self.organization_id = self.organization.get('id') if self.organization else None
        self.orga_sp = self.organization.get('public_service') if self.organization else None
        self.orga_followers = self.organization.get('followers') if self.organization else None
        self.organization = self.organization.get('name') if self.organization else None

        self.concat_title_org = self.title + (' ' + self.organization if self.organization else '')
        self.geozones = '' # TODO
        
        self.created_at = datetime.datetime.fromisoformat(self.created_at)
        if isinstance(self.temporal_coverage_start, str):
            self.temporal_coverage_start = datetime.datetime.fromisoformat(self.temporal_coverage_start)
        if isinstance(self.temporal_coverage_end, str):
            self.temporal_coverage_end = datetime.datetime.fromisoformat(self.temporal_coverage_end)




@dataclasses.dataclass
class Reuse(Base):
    id: str
    title: str
    url: str
    created_at: datetime
    views: int
    followers: int
    featured: int
    datasets: int
    description: str
    organization: str

    orga_followers: int = 0
    organization_id: str = None

    def __post_init__(self):
        # We need to get value from nested organization dict and then set it to str
        self.organization_id = self.organization.get('id') if self.organization else None
        self.orga_followers = self.organization.get('followers') if self.organization else None
        self.organization = self.organization.get('name') if self.organization else None

        self.created_at = datetime.datetime.fromisoformat(self.created_at)



@dataclasses.dataclass
class Organization(Base):
    id: str
    acronym: str
    name: str
    description: str
    url: str
    created_at: datetime
    followers: int
    datasets: int
    orga_sp: int

    def __post_init__(self):
        self.created_at = datetime.datetime.fromisoformat(self.created_at)


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
                dataclass = Dataset
            elif index == 'reuse':
                dataclass = Reuse
            elif index == 'organization':
                dataclass = Organization
            else:
                logging.error(f'Model Deserializer not implemented for index: {index}')
                continue
            data = dataclass.load_fields(val_utf8)
            data = dataclass(**data).to_dict()
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
