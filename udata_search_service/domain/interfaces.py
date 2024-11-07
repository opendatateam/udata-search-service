from abc import ABC, abstractmethod
from typing import Tuple, Optional, List
from udata_search_service.domain.entities import Dataset, Organization, Reuse, Dataservice


class SearchClient(ABC):

    @abstractmethod
    def init_indices(self) -> None:
        pass

    @abstractmethod
    def clean_indices(self) -> None:
        pass

    @abstractmethod
    def index_organization(self, to_index: Organization) -> None:
        pass

    @abstractmethod
    def index_dataset(self, to_index: Dataset) -> None:
        pass

    @abstractmethod
    def index_reuse(self, to_index: Reuse) -> None:
        pass

    @abstractmethod
    def index_dataservice(self, to_index: Dataservice) -> None:
        pass

    @abstractmethod
    def query_organizations(self, query_text: str, offset: int, page_size: int) -> Tuple[int, List[Organization]]:
        pass

    @abstractmethod
    def query_datasets(self, query_text: str, offset: int, page_size: int) -> Tuple[int, List[Dataset]]:
        pass

    @abstractmethod
    def query_reuses(self, query_text: str, offset: int, page_size: int) -> Tuple[int, List[Reuse]]:
        pass

    @abstractmethod
    def query_dataservices(self, query_text: str, offset: int, page_size: int) -> Tuple[int, List[Dataservice]]:
        pass

    @abstractmethod
    def find_one_organization(self, organization_id: str) -> Optional[Organization]:
        pass

    @abstractmethod
    def find_one_dataset(self, dataset_id: str) -> Optional[Dataset]:
        pass

    @abstractmethod
    def find_one_reuse(self, reuse_id: str) -> Optional[Reuse]:
        pass

    @abstractmethod
    def find_one_dataservice(self, dataservice_id: str) -> Optional[Dataservice]:
        pass
