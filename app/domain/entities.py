import dataclasses
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

    acronym: str = None

    def __post_init__(self):
        if isinstance(self.created_at, str):
            self.created_at = isoparse(self.created_at)


@dataclasses.dataclass
class Dataset(EntityBase):
    id: str
    title: str
    acronym: str
    url: str
    created_at: datetime.date
    views: int
    followers: int
    reuses: int
    featured: int
    resources_count: int
    concat_title_org: str
    description: str

    temporal_coverage_start: datetime.date = None
    temporal_coverage_end: datetime.date = None
    granularity: str = None
    geozone: str = None

    orga_sp: int = None
    orga_followers: int = None
    organization_id: str = None
    organization: str = None

    def __post_init__(self):
        if isinstance(self.created_at, str):
            self.created_at = isoparse(self.created_at)
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

    orga_followers: int = None
    organization_id: str = None
    organization: str = None

    def __post_init__(self):
        if isinstance(self.created_at, str):
            self.created_at = isoparse(self.created_at)
