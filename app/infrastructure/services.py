from typing import Tuple, Optional, List
from fastapi import Depends
from app.domain.entities import Dataset, Organization, Reuse
from app.infrastructure.search_clients import ElasticClient


class OrganizationService:

    def __init__(self, search_client: ElasticClient = Depends(ElasticClient)):
        self.search_client = search_client

    def feed(self, organization: Organization) -> None:
        self.search_client.index_organization(organization)

    def search(self, search_text: str, page: int, page_size: int) -> Tuple[List[Organization], int, int]:
        if page > 1:
            offset = page_size * (page - 1)
        else:
            offset = 0

        results_number, search_results = self.search_client.query_organizations(search_text, offset, page_size)
        results = [Organization(**hit) for hit in search_results]
        total_pages = round(results_number / page_size) or 1
        return results, results_number, total_pages

    def find_one(self, organization_id: str) -> Optional[Organization]:
        try:
            return Organization(**self.search_client.find_one_organization(organization_id))
        except TypeError:
            return None


class DatasetService:

    def __init__(self, search_client: ElasticClient = Depends(ElasticClient)):
        self.search_client = search_client

    def feed(self, dataset: Dataset) -> None:
        self.search_client.index_dataset(dataset)

    def search(self, search_text: str, page: int, page_size: int) -> Tuple[List[Dataset], int, int]:
        if page > 1:
            offset = page_size * (page - 1)
        else:
            offset = 0

        results_number, search_results = self.search_client.query_datasets(search_text, offset, page_size)
        results = [Dataset(**hit) for hit in search_results]
        total_pages = round(results_number / page_size) or 1
        return results, results_number, total_pages

    def find_one(self, dataset_id: str) -> Optional[Dataset]:
        try:
            return Dataset(**self.search_client.find_one_dataset(dataset_id))
        except TypeError:
            return None


class ReuseService:

    def __init__(self, search_client: ElasticClient = Depends(ElasticClient)):
        self.search_client = search_client

    def feed(self, reuse: Reuse) -> None:
        self.search_client.index_reuse(reuse)

    def search(self, search_text: str, page: int, page_size: int) -> Tuple[List[Reuse], int, int]:
        if page > 1:
            offset = page_size * (page - 1)
        else:
            offset = 0

        results_number, search_results = self.search_client.query_reuses(search_text, offset, page_size)
        results = [Reuse(**hit) for hit in search_results]
        total_pages = round(results_number / page_size) or 1
        return results, results_number, total_pages

    def find_one(self, reuse_id: str) -> Optional[Reuse]:
        try:
            return Reuse(**self.search_client.find_one_reuse(reuse_id))
        except TypeError:
            return None
