from typing import Tuple, Optional, List
from app.domain.entities import Dataset, Organization, Reuse
from app.infrastructure.search_clients import ElasticClient


class OrganizationService:

    def __init__(self, search_client: ElasticClient):
        self.search_client = search_client

    def feed(self, organization: Organization) -> None:
        self.search_client.index_organization(organization)

    def search(self, filters: dict) -> Tuple[List[Organization], int, int]:
        page = filters.pop('page')
        page_size = filters.pop('page_size')
        search_text = filters.pop('q')

        if page > 1:
            offset = page_size * (page - 1)
        else:
            offset = 0

        results_number, search_results = self.search_client.query_organizations(search_text, offset, page_size, filters)
        results = [Organization.load_from_dict(hit) for hit in search_results]
        total_pages = round(results_number / page_size) or 1
        return results, results_number, total_pages

    def find_one(self, organization_id: str) -> Optional[Organization]:
        try:
            return Organization.load_from_dict(self.search_client.find_one_organization(organization_id))
        except TypeError:
            return None


class DatasetService:

    def __init__(self, search_client: ElasticClient):
        self.search_client = search_client

    def feed(self, dataset: Dataset) -> None:
        self.search_client.index_dataset(dataset)

    def search(self, filters: dict) -> Tuple[List[Dataset], int, int]:
        page = filters.pop('page')
        page_size = filters.pop('page_size')
        search_text = filters.pop('q')

        if page > 1:
            offset = page_size * (page - 1)
        else:
            offset = 0

        if filters['temporal_coverage']:
            self.parse_temporal_value(filters)

        results_number, search_results = self.search_client.query_datasets(search_text, offset, page_size, filters)
        results = [Dataset.load_from_dict(hit) for hit in search_results]
        total_pages = round(results_number / page_size) or 1
        return results, results_number, total_pages

    def find_one(self, dataset_id: str) -> Optional[Dataset]:
        try:
            return Dataset.load_from_dict(self.search_client.find_one_dataset(dataset_id))
        except TypeError:
            return None

    @staticmethod
    def parse_temporal_value(filters):
        parts = filters.pop('temporal_coverage')
        filters['temporal_coverage_start'] = parts[:10]
        filters['temporal_coverage_end'] = parts[11:]


class ReuseService:

    def __init__(self, search_client: ElasticClient):
        self.search_client = search_client

    def feed(self, reuse: Reuse) -> None:
        self.search_client.index_reuse(reuse)

    def search(self, filters: dict) -> Tuple[List[Reuse], int, int]:
        page = filters.pop('page')
        page_size = filters.pop('page_size')
        search_text = filters.pop('q')

        if page > 1:
            offset = page_size * (page - 1)
        else:
            offset = 0

        results_number, search_results = self.search_client.query_reuses(search_text, offset, page_size, filters)
        results = [Reuse.load_from_dict(hit) for hit in search_results]
        total_pages = round(results_number / page_size) or 1
        return results, results_number, total_pages

    def find_one(self, reuse_id: str) -> Optional[Reuse]:
        try:
            return Reuse.load_from_dict(self.search_client.find_one_reuse(reuse_id))
        except TypeError:
            return None
