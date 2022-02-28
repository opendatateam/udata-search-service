from typing import Tuple, Optional, List
from udata_search_service.domain.entities import Dataset, Organization, Reuse
from udata_search_service.infrastructure.search_clients import ElasticClient


class OrganizationService:

    def __init__(self, search_client: ElasticClient):
        self.search_client = search_client

    def feed(self, organization: Organization) -> None:
        self.search_client.index_organization(organization)

    def search(self, filters: dict) -> Tuple[List[Organization], int, int]:
        page = filters.pop('page')
        page_size = filters.pop('page_size')
        search_text = filters.pop('q')
        sort = self.format_sort(filters.pop('sort', None))

        if page > 1:
            offset = page_size * (page - 1)
        else:
            offset = 0

        self.format_filters(filters)

        results_number, search_results = self.search_client.query_organizations(search_text, offset, page_size, filters, sort)
        results = [Organization.load_from_dict(hit) for hit in search_results]
        total_pages = round(results_number / page_size) or 1
        return results, results_number, total_pages

    def find_one(self, organization_id: str) -> Optional[Organization]:
        try:
            return Organization.load_from_dict(self.search_client.find_one_organization(organization_id))
        except TypeError:
            return None

    @staticmethod
    def format_filters(filters):
        filtered = {k: v for k, v in filters.items() if v is not None}
        filters.clear()
        filters.update(filtered)

    @staticmethod
    def format_sort(sort):
        if sort and 'created' in sort:
            sort = sort.replace('created', 'created_at')
        return sort


class DatasetService:

    def __init__(self, search_client: ElasticClient):
        self.search_client = search_client

    def feed(self, dataset: Dataset) -> None:
        self.search_client.index_dataset(dataset)

    def search(self, filters: dict) -> Tuple[List[Dataset], int, int]:
        page = filters.pop('page')
        page_size = filters.pop('page_size')
        search_text = filters.pop('q')
        sort = self.format_sort(filters.pop('sort', None))

        if page > 1:
            offset = page_size * (page - 1)
        else:
            offset = 0

        self.format_filters(filters)

        results_number, search_results = self.search_client.query_datasets(search_text, offset, page_size, filters, sort)
        results = [Dataset.load_from_dict(hit) for hit in search_results]
        total_pages = round(results_number / page_size) or 1
        return results, results_number, total_pages

    def find_one(self, dataset_id: str) -> Optional[Dataset]:
        try:
            return Dataset.load_from_dict(self.search_client.find_one_dataset(dataset_id))
        except TypeError:
            return None

    @staticmethod
    def format_filters(filters):
        if filters['temporal_coverage']:
            parts = filters.pop('temporal_coverage')
            filters['temporal_coverage_start'] = parts[:10]
            filters['temporal_coverage_end'] = parts[11:]
        if filters['tag']:
            filters['tags'] = filters.pop('tag')
        if filters['geozone']:
            filters['geozones'] = filters.pop('geozone')
        if filters['schema_']:
            filters['schema'] = filters.pop('schema_')
        filtered = {k: v for k, v in filters.items() if v is not None}
        filters.clear()
        filters.update(filtered)

    @staticmethod
    def format_sort(sort):
        if sort is not None and 'created' in sort:
            sort = sort.replace('created', 'created_at')
        return sort


class ReuseService:

    def __init__(self, search_client: ElasticClient):
        self.search_client = search_client

    def feed(self, reuse: Reuse) -> None:
        self.search_client.index_reuse(reuse)

    def search(self, filters: dict) -> Tuple[List[Reuse], int, int]:
        page = filters.pop('page')
        page_size = filters.pop('page_size')
        search_text = filters.pop('q')
        sort = self.format_sort(filters.pop('sort', None))

        if page > 1:
            offset = page_size * (page - 1)
        else:
            offset = 0

        self.format_filters(filters)

        results_number, search_results = self.search_client.query_reuses(search_text, offset, page_size, filters, sort)
        results = [Reuse.load_from_dict(hit) for hit in search_results]
        total_pages = round(results_number / page_size) or 1
        return results, results_number, total_pages

    def find_one(self, reuse_id: str) -> Optional[Reuse]:
        try:
            return Reuse.load_from_dict(self.search_client.find_one_reuse(reuse_id))
        except TypeError:
            return None

    @staticmethod
    def format_filters(filters):
        if filters['tag']:
            filters['tags'] = filters.pop('tag')
        filtered = {k: v for k, v in filters.items() if v is not None}
        filters.clear()
        filters.update(filtered)

    @staticmethod
    def format_sort(sort):
        if sort is not None and 'created' in sort:
            sort = sort.replace('created', 'created_at')
        return sort
