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

    search_client.clean_indices()

    yield

    SearchableDataset.delete_indices(search_client.es)
    SearchableReuse.delete_indices(search_client.es)
    SearchableOrganization.delete_indices(search_client.es)


@pytest.fixture
def faker():
    return Faker()
