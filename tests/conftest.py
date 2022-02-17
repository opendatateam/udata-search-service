from faker import Faker
from elasticsearch_dsl import Index
import pytest
from udata_search_service import create_app
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
    if Index('dataset').exists():
        Index('dataset').delete()
    if Index('reuse').exists():
        Index('reuse').delete()
    if Index('organization').exists():
        Index('organization').delete()

    SearchableDataset.init()
    SearchableReuse.init()
    SearchableOrganization.init()
    yield
    Index('dataset').delete()
    Index('reuse').delete()
    Index('organization').delete()


@pytest.fixture
def faker():
    return Faker()
