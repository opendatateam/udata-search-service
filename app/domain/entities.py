import dataclasses
from datetime import datetime


@dataclasses.dataclass
class Organization:
    id: str
    name: str
    acronym: str
    description: str
    url: str
    orga_sp: int
    created_at: str
    followers: int
    datasets: int

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
    views: int
    followers: int
    reuses: int
    featured: int
    resources_count: int
    concat_title_org: str
    organization_id: str
    temporal_coverage_start: str
    temporal_coverage_end: str
    granularity: str
    geozones: str
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
    views: int
    followers: int
    datasets: int
    featured: int
    organization_id: str
    description: str
    organization: str

    def __post_init__(self):
        if isinstance(self.created_at, datetime):
            self.created_at = self.created_at.strftime('%Y-%m-%d')
