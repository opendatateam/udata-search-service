import time
from app.domain.entities import Dataset, Organization, Reuse


def test_general_search_with_and_without_query(app, client, search_client, faker):
    for i in range(4):
        title = 'test-{0}'.format(i) if i % 2 else faker.word()
        acronym = faker.company_suffix()
        organization = 'test-{0}'.format(faker.company()) if i % 2 else faker.company()
        search_client.index_dataset(Dataset(
            id=faker.md5(),
            title=title,
            acronym=acronym,
            url=faker.uri(),
            created_at=faker.date(),
            orga_sp=4 if i % 2 else 1,
            orga_followers=faker.random_int(),
            dataset_views=faker.random_int(),
            dataset_followers=faker.random_int(),
            dataset_reuses=faker.random_int(),
            dataset_featured=faker.random_int(),
            resources_count=faker.random_int(min=1, max=15),
            concat_title_org=title + ' ' + acronym + ' ' + organization,
            organization_id=faker.md5(),
            temporal_coverage_start=None,
            temporal_coverage_end=None,
            spatial_granularity=None,
            spatial_zones=None,
            description=faker.sentence(nb_words=10),
            organization=organization
        ))

        search_client.index_organization(Organization(
            id=faker.md5(),
            name=organization,
            description=faker.sentence(nb_words=10),
            url=faker.uri(),
            orga_sp=4 if i % 2 else 1,
            created_at=faker.date(),
            orga_followers=faker.random_int(),
            orga_datasets=faker.random_int()
        ))

        search_client.index_reuse(Reuse(
            id=faker.md5(),
            title='test-{0}'.format(i) if i % 2 else faker.word(),
            url=faker.uri(),
            created_at=faker.date(),
            orga_followers=faker.random_int(),
            reuse_views=faker.random_int(),
            reuse_followers=faker.random_int(),
            reuse_datasets=faker.random_int(),
            reuse_featured=faker.random_int(),
            organization_id=faker.md5(),
            description=faker.sentence(nb_words=10),
            organization=organization
        ))
    # Without this, ElasticSearch does not seem to have the time to index.
    time.sleep(2)

    # Should return only the object with string 'test' in their titles/names
    results_number, res = search_client.query_datasets('test', 0, 20, {})
    assert results_number == 2
    results_number, res = search_client.query_organizations('test', 0, 20, {})
    assert results_number == 2
    results_number, res = search_client.query_reuses('test', 0, 20, {})
    assert results_number == 2

    # Should return all objects, as query text is none
    results_number, res = search_client.query_datasets(None, 0, 20, {})
    assert results_number == 4
    results_number, res = search_client.query_organizations(None, 0, 20, {})
    assert results_number == 4
    results_number, res = search_client.query_reuses(None, 0, 20, {})
    assert results_number == 4


def test_search_with_filters(app, client, search_client, faker):
    for i in range(4):
        title = 'test-{0}'.format(faker.word())
        acronym = faker.company_suffix()
        organization = 'test-{0}'.format(faker.company())
        search_client.index_dataset(Dataset(
            id=faker.md5(),
            title=title,
            acronym=acronym,
            url=faker.uri(),
            created_at=faker.date(),
            orga_sp=4 if i % 2 else 1,
            orga_followers=faker.random_int(),
            dataset_views=faker.random_int(),
            dataset_followers=faker.random_int(),
            dataset_reuses=faker.random_int(),
            dataset_featured=faker.random_int(),
            resources_count=faker.random_int(min=1, max=15),
            concat_title_org=title + ' ' + acronym + ' ' + organization,
            organization_id=faker.md5(),
            temporal_coverage_start=None,
            temporal_coverage_end=None,
            spatial_granularity=None,
            spatial_zones=None,
            description=faker.sentence(nb_words=10),
            organization=organization
        ))

        search_client.index_organization(Organization(
            id=faker.md5(),
            name=organization,
            description=faker.sentence(nb_words=10),
            url=faker.uri(),
            orga_sp=4 if i % 2 else 1,
            created_at=faker.date(),
            orga_followers=faker.random_int(),
            orga_datasets=faker.random_int()
        ))

    # Without this, ElasticSearch does not seem to have the time to index.
    time.sleep(2)

    results_number, res = search_client.query_datasets('test', 0, 20, {})
    assert results_number == 4
    results_number, res = search_client.query_datasets('test', 0, 20, {'orga_sp': 4})
    assert results_number == 2
    results_number, res = search_client.query_organizations('test', 0, 20, {})
    assert results_number == 4
    results_number, res = search_client.query_organizations('test', 0, 20, {'orga_sp': 4})
    assert results_number == 2


def test_search_datasets_with_temporal_filters(app, client, search_client, faker):
    for i in range(4):
        title = 'test-{0}'.format(faker.word())
        acronym = faker.company_suffix()
        organization = 'test-{0}'.format(faker.company())
        search_client.index_dataset(Dataset(
            id=faker.md5(),
            title=title,
            acronym=acronym,
            url=faker.uri(),
            created_at=faker.date(),
            orga_sp=4 if i % 2 else 1,
            orga_followers=faker.random_int(),
            dataset_views=faker.random_int(),
            dataset_followers=faker.random_int(),
            dataset_reuses=faker.random_int(),
            dataset_featured=faker.random_int(),
            resources_count=faker.random_int(min=1, max=15),
            concat_title_org=title + ' ' + acronym + ' ' + organization,
            organization_id=faker.md5(),
            temporal_coverage_start='2021-12-02' if i % 2 else '2020-02-24',
            temporal_coverage_end='2022-01-13' if i % 2 else '2020-03-13',
            spatial_granularity=None,
            spatial_zones=None,
            description=faker.sentence(nb_words=10),
            organization=organization
        ))
    # Without this, ElasticSearch does not seem to have the time to index.
    time.sleep(2)

    results_number, res = search_client.query_datasets('test', 0, 20, {})
    assert results_number == 4
    results_number, res = search_client.query_datasets('test', 0, 20, {'temporal_coverage_start': '2020-01-29', 'temporal_coverage_end': '2020-04-15'})
    assert results_number == 2
