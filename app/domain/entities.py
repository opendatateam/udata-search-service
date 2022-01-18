import dataclasses
from datetime import datetime


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
    created_at: str
    followers: int
    datasets: int

    acronym: str = None

    def __post_init__(self):
        if isinstance(self.created_at, datetime):
            self.created_at = self.created_at.strftime('%Y-%m-%d')


@dataclasses.dataclass
class Dataset(EntityBase):
    id: str
    title: str
    acronym: str
    url: str
    created_at: str
    views: int
    followers: int
    reuses: int
    featured: int
    resources_count: int
    concat_title_org: str
    description: str

    temporal_coverage_start: str = None
    temporal_coverage_end: str = None
    granularity: str = None
    geozones: str = None

    orga_sp: int = None
    orga_followers: int = None
    organization_id: str = None
    organization: str = None

    def __post_init__(self):
        if isinstance(self.created_at, datetime):
            self.created_at = self.created_at.strftime('%Y-%m-%d')


@dataclasses.dataclass
class Reuse(EntityBase):
    id: str
    title: str
    url: str
    created_at: str
    views: int
    followers: int
    datasets: int
    featured: int
    description: str

    orga_followers: int = None
    organization_id: str = None
    organization: str = None

    def __post_init__(self):
        if isinstance(self.created_at, datetime):
            self.created_at = self.created_at.strftime('%Y-%m-%d')
