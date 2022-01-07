import pytest
from app import create_app
from app.config import Testing
from app.domain.entities import Dataset
from app.domain.interfaces import SearchClient


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
def single_dataset():
    return Dataset(
        id='test-id',
        title='test-dataset-title',
        url='http://local.dev',
        description='test-dataset-description',
        orga_sp=1,
        orga_followers=1,
        dataset_views=1,
        dataset_followers=1,
        dataset_reuses=0,
        dataset_featured=1,
        temporal_coverage_start='',
        temporal_coverage_end='',
        spatial_granularity='',
        spatial_zones='',
        concat_title_org='test-dataset-title orga',
        organization_id='orga-id',
        organization='orga'
    )


@pytest.fixture
def search_client(single_dataset):
    class TestSearchClient(SearchClient):
        def delete_index(self):
            pass

        def create_index(self) -> None:
            pass

        def index_dataset(self, to_index):
            pass

        def query_datasets(self, query_text, offset, page_size):
            return 3, [single_dataset, single_dataset, single_dataset]

        def find_one(self, dataset_id):
            return single_dataset

    return TestSearchClient()
