import datetime
import factory

from udata_search_service.domain.entities import Dataset, Organization, Reuse


class DatasetFactory(factory.Factory):
    class Meta:
        model = Dataset

    id = factory.Faker('md5')
    title = factory.Faker('sentence')
    description = factory.Faker('text')
    acronym = factory.Faker('company_suffix')
    url = factory.Faker('url')
    created_at = factory.LazyFunction(datetime.datetime.now)
    orga_sp = 4
    orga_followers = factory.Faker('random_int')
    views = factory.Faker('random_int')
    followers = factory.Faker('random_int')
    reuses = factory.Faker('random_int')
    featured = factory.Faker('random_int')
    resources_count = factory.Faker('random_int', min=1, max=15)
    organization = factory.Faker('md5')
    organization_name = factory.Faker('company')
    format = ['pdf']
    frequency = 'unknown'
    concat_title_org = factory.LazyAttribute(lambda obj: f'{obj.title} {obj.acronym} {obj.organization_name}')
    badges = []
    tags = []
    license = factory.Faker('word')
    temporal_coverage_start = factory.Faker('past_datetime')
    temporal_coverage_end = factory.Faker('past_datetime')
    granularity = factory.Faker('word')
    geozones = factory.Faker('word')
    owner = factory.Faker('md5')


class OrganizationFactory(factory.Factory):
    class Meta:
        model = Organization

    id = factory.Faker('md5')
    name = factory.Faker('company')
    description = factory.Faker('text')
    url = factory.Faker('url')
    orga_sp = 4
    created_at = factory.LazyFunction(datetime.datetime.now)
    followers = factory.Faker('random_int')
    datasets = factory.Faker('random_int')
    views = factory.Faker('random_int')
    reuses = factory.Faker('random_int')


class ReuseFactory(factory.Factory):
    class Meta:
        model = Reuse

    id = factory.Faker('md5')
    title = factory.Faker('sentence')
    description = factory.Faker('text')
    url = factory.Faker('url')
    created_at = factory.LazyFunction(datetime.datetime.now)
    orga_followers = factory.Faker('random_int')
    views = factory.Faker('random_int')
    followers = factory.Faker('random_int')
    datasets = factory.Faker('random_int')
    featured = factory.Faker('random_int')
    organization = factory.Faker('md5')
    organization_name = factory.Faker('company')
    type = factory.Faker('word')
    topic = factory.Faker('word')
    owner = factory.Faker('md5')
