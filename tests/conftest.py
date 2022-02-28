from datetime import datetime

from faker import Faker
from elasticsearch_dsl import Index
import pytest
from udata_search_service.app import create_app
from udata_search_service.config import Testing
from udata_search_service.infrastructure.search_clients import SearchableDataset, SearchableReuse, SearchableOrganization, ElasticClient


@pytest.fixture
def app():
    app = create_app(Testing)
    yield app
    app.container.unwire()


@pytest.fixture
def client(app):
    with app.test_client() as client:
        return client


@pytest.fixture
def search_client(app):
    return ElasticClient(url=app.config['ELASTICSEARCH_URL'])


@pytest.fixture(autouse=True)
def db(search_client):
    suffix_name = '-' + datetime.now().strftime('%Y-%m-%d-%H-%M')

    search_client.delete_index_with_alias('dataset')
    SearchableDataset._index._name = 'dataset' + suffix_name
    SearchableDataset.init()
    Index('dataset' + suffix_name).put_alias(name='dataset')

    search_client.delete_index_with_alias('reuse')
    SearchableReuse._index._name = 'reuse' + suffix_name
    SearchableReuse.init()
    Index('reuse' + suffix_name).put_alias(name='reuse')

    search_client.delete_index_with_alias('organization')
    SearchableOrganization._index._name = 'organization' + suffix_name
    SearchableOrganization.init()
    Index('organization' + suffix_name).put_alias(name='organization')

    yield

    Index('dataset' + suffix_name).delete()
    Index('reuse' + suffix_name).delete()
    Index('organization' + suffix_name).delete()


@pytest.fixture
def faker():
    return Faker()
