import dataclasses
from typing import List
from datetime import datetime
from dateutil.parser import isoparse


@dataclasses.dataclass
class EntityBase():

    @classmethod
    def load_from_dict(cls, data):
        fields = [f.name for f in dataclasses.fields(cls)]
        data = {key: data[key] for key in data if key in fields}
        return cls(**data)

    def to_dict(self):
        return dataclasses.asdict(self)


@dataclasses.dataclass
class Organization(EntityBase):
    id: str
    name: str
    description: str
    url: str
    orga_sp: int
    created_at: datetime.date
    followers: int
    datasets: int
    views: int
    reuses: int

    badges: List[str] = None
    acronym: str = None

    def __post_init__(self):
        if isinstance(self.created_at, str):
            self.created_at = isoparse(self.created_at)


@dataclasses.dataclass
class Dataset(EntityBase):
    id: str
    title: str
    url: str
    created_at: datetime.date
    frequency: str
    format: List[str]
    views: int
    followers: int
    reuses: int
    featured: int
    resources_count: int
    concat_title_org: str
    description: str

    last_update: datetime.date = None
    acronym: str = None
    badges: List[str] = None
    tags: List[str] = None
    license: str = None
    temporal_coverage_start: datetime.date = None
    temporal_coverage_end: datetime.date = None
    granularity: str = None
    geozones: List[str] = None
    schema: List[str] = None
    topics: List[str] = None
    resources_ids: List[str] = None
    resources_titles: List[str] = None

    orga_sp: int = None
    orga_followers: int = None
    organization: str = None
    organization_name: str = None
    organization_badges: List[str] = None
    owner: str = None

    def __post_init__(self):
        if isinstance(self.created_at, str):
            self.created_at = isoparse(self.created_at)
        if isinstance(self.last_update, str):
            self.last_update = isoparse(self.last_update)
        if isinstance(self.temporal_coverage_start, str):
            self.temporal_coverage_start = isoparse(self.temporal_coverage_start)
        if isinstance(self.temporal_coverage_end, str):
            self.temporal_coverage_end = isoparse(self.temporal_coverage_end)


@dataclasses.dataclass
class Reuse(EntityBase):
    id: str
    title: str
    url: str
    created_at: datetime.date
    views: int
    followers: int
    datasets: int
    featured: int
    description: str
    type: str
    topic: str

    archived: datetime.date = None
    tags: List[str] = None
    badges: List[str] = None
    orga_followers: int = None
    organization: str = None
    organization_name: str = None
    organization_badges: List[str] = None
    owner: str = None

    def __post_init__(self):
        if isinstance(self.created_at, str):
            self.created_at = isoparse(self.created_at)
        if isinstance(self.archived, str):
            self.archived = isoparse(self.archived)

@dataclasses.dataclass
class Dataservice(EntityBase):
    id: str
    title: str
    description: str
    description_length: float
    created_at: datetime.date

    views: int = 0
    followers: int = 0
    is_restricted: bool = None
    orga_followers: int = None
    organization: str = None
    organization_name: str = None
    owner: str = None
    tags: List[str] = None

    def __post_init__(self):
        if isinstance(self.created_at, str):
            self.created_at = isoparse(self.created_at)
