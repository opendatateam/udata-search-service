import dataclasses
from datetime import datetime


@dataclasses.dataclass
class Organization:
    id: str
    name: str
    description: str
    url: str
    orga_sp: int
    created_at: str
    orga_followers: int
    orga_datasets: int

    def __post_init__(self):
        if isinstance(self.created_at, datetime):
            self.created_at = self.created_at.strftime('%Y-%m-%d')


@dataclasses.dataclass
class Dataset:
    id: str
    title: str
    acronym: str
    url: str
    created_at: str
    orga_sp: int
    orga_followers: int
    dataset_views: int
    dataset_followers: int
    dataset_reuses: int
    dataset_featured: int
    resources_count: int
    concat_title_org: str
    organization_id: str
    temporal_coverage_start: str
    temporal_coverage_end: str
    spatial_granularity: str
    spatial_zones: str
    description: str
    organization: str

    def __post_init__(self):
        if isinstance(self.created_at, datetime):
            self.created_at = self.created_at.strftime('%Y-%m-%d')


@dataclasses.dataclass
class Reuse:
    id: str
    title: str
    url: str
    created_at: str
    orga_followers: int
    reuse_views: int
    reuse_followers: int
    reuse_datasets: int
    reuse_featured: int
    organization_id: str
    description: str
    organization: str

    def __post_init__(self):
        if isinstance(self.created_at, datetime):
            self.created_at = self.created_at.strftime('%Y-%m-%d')
