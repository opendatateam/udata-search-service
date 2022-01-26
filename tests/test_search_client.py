import datetime
import time

from app.domain.entities import Dataset, Organization, Reuse


def test_general_search_with_and_without_query(app, client, search_client, faker):
    for i in range(4):
        title = 'test-{0}'.format(i) if i % 2 else faker.word(ext_word_list=['abc', 'def', 'hij', 'klm', 'nop', 'qrs', 'tuv'])
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
            views=faker.random_int(),
            followers=faker.random_int(),
            reuses=faker.random_int(),
            featured=faker.random_int(),
            resources_count=faker.random_int(min=1, max=15),
            concat_title_org=title + ' ' + acronym + ' ' + organization,
            organization=faker.md5(),
            description=faker.sentence(nb_words=10),
            organization_name=organization,
            format=['pdf'],
            frequency=faker.word()
        ))

        search_client.index_organization(Organization(
            id=faker.md5(),
            name=organization,
            description=faker.sentence(nb_words=10),
            url=faker.uri(),
            orga_sp=4 if i % 2 else 1,
            created_at=faker.date(),
            followers=faker.random_int(),
            datasets=faker.random_int()
        ))

        search_client.index_reuse(Reuse(
            id=faker.md5(),
            title='test-{0}'.format(i) if i % 2 else faker.word(ext_word_list=['abc', 'def', 'hij', 'klm', 'nop', 'qrs', 'tuv']),
            url=faker.uri(),
            created_at=faker.date(),
            orga_followers=faker.random_int(),
            views=faker.random_int(),
            followers=faker.random_int(),
            datasets=faker.random_int(),
            featured=faker.random_int(),
            organization=faker.md5(),
            description=faker.sentence(nb_words=10),
            organization_name=organization,
            type=faker.word(),
            topic=faker.word()
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


def test_search_with_orga_sp_filter(app, client, search_client, faker):
    for i in range(4):
        title = 'test-{0}'.format(faker.word(ext_word_list=['abc', 'def', 'hij', 'klm', 'nop', 'qrs', 'tuv']))
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
            views=faker.random_int(),
            followers=faker.random_int(),
            reuses=faker.random_int(),
            featured=faker.random_int(),
            resources_count=faker.random_int(min=1, max=15),
            concat_title_org=title + ' ' + acronym + ' ' + organization,
            organization=faker.md5(),
            description=faker.sentence(nb_words=10),
            organization_name=organization,
            format=[faker.word()],
            frequency=faker.word()
        ))

        search_client.index_organization(Organization(
            id=faker.md5(),
            name=organization,
            description=faker.sentence(nb_words=10),
            url=faker.uri(),
            orga_sp=4 if i % 2 else 1,
            created_at=faker.date(),
            followers=faker.random_int(),
            datasets=faker.random_int()
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


def test_search_with_orga_id_filter(app, client, search_client, faker):
    for i in range(4):
        title = 'test-{0}'.format(faker.word(ext_word_list=['abc', 'def', 'hij', 'klm', 'nop', 'qrs', 'tuv']))
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
            views=faker.random_int(),
            followers=faker.random_int(),
            reuses=faker.random_int(),
            featured=faker.random_int(),
            resources_count=faker.random_int(min=1, max=15),
            concat_title_org=title + ' ' + acronym + ' ' + organization,
            organization=faker.md5(),
            description=faker.sentence(nb_words=10),
            organization_name=organization,
            format=['pdf'],
            frequency=faker.word()
        ))

        search_client.index_reuse(Reuse(
            id=faker.md5(),
            title='test-{0}'.format(i) if i % 2 else faker.word(ext_word_list=['abc', 'def', 'hij', 'klm', 'nop', 'qrs', 'tuv']),
            url=faker.uri(),
            created_at=faker.date(),
            orga_followers=faker.random_int(),
            views=faker.random_int(),
            followers=faker.random_int(),
            datasets=faker.random_int(),
            featured=faker.random_int(),
            organization=faker.md5(),
            description=faker.sentence(nb_words=10),
            organization_name=organization,
            type=faker.word(),
            topic=faker.word()
        ))

    search_client.index_dataset(Dataset(
        id=faker.md5(),
        title='test-orga-id',
        url=faker.uri(),
        created_at=faker.date(),
        orga_sp=4 if i % 2 else 1,
        orga_followers=faker.random_int(),
        views=faker.random_int(),
        followers=faker.random_int(),
        reuses=faker.random_int(),
        featured=faker.random_int(),
        resources_count=faker.random_int(min=1, max=15),
        concat_title_org='test-orga-id test_orga_name',
        organization='64f01c248bf99eab7c197717',
        description=faker.sentence(nb_words=10),
        organization_name='test_orga_name',
        format=[faker.word()],
        frequency=faker.word()
    ))

    search_client.index_reuse(Reuse(
        id=faker.md5(),
        title='test-{0}'.format(i) if i % 2 else faker.word(
            ext_word_list=['abc', 'def', 'hij', 'klm', 'nop', 'qrs', 'tuv']),
        url=faker.uri(),
        created_at=faker.date(),
        orga_followers=faker.random_int(),
        views=faker.random_int(),
        followers=faker.random_int(),
        datasets=faker.random_int(),
        featured=faker.random_int(),
        organization='77f01c346bf99eab7c198891',
        description=faker.sentence(nb_words=10),
        organization_name='test_orga_name',
        type=faker.word(),
        topic=faker.word()
    ))

    # Without this, ElasticSearch does not seem to have the time to index.
    time.sleep(2)

    results_number, res = search_client.query_datasets('test', 0, 20, {})
    assert results_number == 5
    results_number, res = search_client.query_datasets('test', 0, 20, {'organization': '64f01c248bf99eab7c197717'})
    assert results_number == 1
    results_number, res = search_client.query_reuses('test', 0, 20, {})
    assert results_number == 5
    results_number, res = search_client.query_reuses('test', 0, 20, {'organization': '77f01c346bf99eab7c198891'})
    assert results_number == 1


def test_search_with_owner_filter(app, client, search_client, faker):
    for i in range(4):
        title = 'test-{0}'.format(faker.word(ext_word_list=['abc', 'def', 'hij', 'klm', 'nop', 'qrs', 'tuv']))
        search_client.index_dataset(Dataset(
            id=faker.md5(),
            title=title,
            url=faker.uri(),
            created_at=faker.date(),
            views=faker.random_int(),
            followers=faker.random_int(),
            reuses=faker.random_int(),
            featured=faker.random_int(),
            resources_count=faker.random_int(min=1, max=15),
            concat_title_org=title,
            description=faker.sentence(nb_words=10),
            format=['pdf'],
            frequency=faker.word(),
            owner=faker.md5()
        ))

        search_client.index_reuse(Reuse(
            id=faker.md5(),
            title='test-{0}'.format(faker.word(ext_word_list=['abc', 'def', 'hij', 'klm', 'nop', 'qrs', 'tuv'])),
            url=faker.uri(),
            created_at=faker.date(),
            views=faker.random_int(),
            followers=faker.random_int(),
            datasets=faker.random_int(),
            featured=faker.random_int(),
            description=faker.sentence(nb_words=10),
            type=faker.word(),
            topic=faker.word(),
            owner=faker.md5()
        ))

    search_client.index_dataset(Dataset(
        id=faker.md5(),
        title='test-data',
        url=faker.uri(),
        created_at=faker.date(),
        views=faker.random_int(),
        followers=faker.random_int(),
        reuses=faker.random_int(),
        featured=faker.random_int(),
        resources_count=faker.random_int(min=1, max=15),
        concat_title_org='test-orga-id',
        description=faker.sentence(nb_words=10),
        format=[faker.word()],
        frequency=faker.word(),
        owner='64f01c248bf99eab7c197717'
    ))

    search_client.index_reuse(Reuse(
        id=faker.md5(),
        title='test-reuse',
        url=faker.uri(),
        created_at=faker.date(),
        views=faker.random_int(),
        followers=faker.random_int(),
        datasets=faker.random_int(),
        featured=faker.random_int(),
        description=faker.sentence(nb_words=10),
        type=faker.word(),
        topic=faker.word(),
        owner='77f01c346bf99eab7c198891'
    ))

    # Without this, ElasticSearch does not seem to have the time to index.
    time.sleep(2)

    results_number, res = search_client.query_datasets('test', 0, 20, {})
    assert results_number == 5
    results_number, res = search_client.query_datasets('test', 0, 20, {'owner': '64f01c248bf99eab7c197717'})
    assert results_number == 1
    results_number, res = search_client.query_reuses('test', 0, 20, {})
    assert results_number == 5
    results_number, res = search_client.query_reuses('test', 0, 20, {'owner': '77f01c346bf99eab7c198891'})
    assert results_number == 1


def test_search_datasets_with_temporal_filters(app, client, search_client, faker):
    for i in range(4):
        title = 'test-{0}'.format(faker.word(ext_word_list=['abc', 'def', 'hij', 'klm', 'nop', 'qrs', 'tuv']))
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
            views=faker.random_int(),
            followers=faker.random_int(),
            reuses=faker.random_int(),
            featured=faker.random_int(),
            resources_count=faker.random_int(min=1, max=15),
            concat_title_org=title + ' ' + acronym + ' ' + organization,
            organization=faker.md5(),
            temporal_coverage_start=datetime.date(2021, 12, 2) if i % 2 else datetime.date(2020, 2, 24),
            temporal_coverage_end=datetime.date(2022, 1, 1) if i % 2 else datetime.date(2022, 2, 13),
            description=faker.sentence(nb_words=10),
            organization_name=organization,
            format=['pdf'],
            frequency=faker.word()
        ))
    # Without this, ElasticSearch does not seem to have the time to index.
    time.sleep(2)

    results_number, res = search_client.query_datasets('test', 0, 20, {})
    assert results_number == 4
    results_number, res = search_client.query_datasets('test', 0, 20, {
        'temporal_coverage_start': datetime.date(2020, 2, 25),
        'temporal_coverage_end': datetime.date(2020, 3, 10)
    })
    assert results_number == 2
    results_number, res = search_client.query_datasets('test', 0, 20, {
        'temporal_coverage_start': datetime.date(2019, 2, 25),
        'temporal_coverage_end': datetime.date(2020, 3, 10)
    })
    assert results_number == 0


def test_search_with_tag_filter(app, client, search_client, faker):
    for i in range(4):
        title = 'test-{0}'.format(faker.word(ext_word_list=['abc', 'def', 'hij', 'klm', 'nop', 'qrs', 'tuv']))
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
            views=faker.random_int(),
            followers=faker.random_int(),
            reuses=faker.random_int(),
            featured=faker.random_int(),
            resources_count=faker.random_int(min=1, max=15),
            concat_title_org=title + ' ' + acronym + ' ' + organization,
            organization=faker.md5(),
            description=faker.sentence(nb_words=10),
            organization_name=organization,
            format=['pdf'],
            frequency=faker.word(),
            tags=['test-tag'] if i % 2 else ['not-test-tag']
        ))

        search_client.index_reuse(Reuse(
            id=faker.md5(),
            title='test-{0}'.format(i) if i % 2 else faker.word(ext_word_list=['abc', 'def', 'hij', 'klm', 'nop', 'qrs', 'tuv']),
            url=faker.uri(),
            created_at=faker.date(),
            orga_followers=faker.random_int(),
            views=faker.random_int(),
            followers=faker.random_int(),
            datasets=faker.random_int(),
            featured=faker.random_int(),
            organization=faker.md5(),
            description=faker.sentence(nb_words=10),
            organization_name=organization,
            type=faker.word(),
            topic=faker.word(),
            tags=['test-tag'] if i % 2 else ['not-test-tag']
        ))

    # Without this, ElasticSearch does not seem to have the time to index.
    time.sleep(2)

    results_number, res = search_client.query_datasets('test', 0, 20, {})
    assert results_number == 4
    results_number, res = search_client.query_datasets('test', 0, 20, {'tags': 'test-tag'})
    assert results_number == 2
    results_number, res = search_client.query_reuses('test', 0, 20, {})
    assert results_number == 4
    results_number, res = search_client.query_reuses('test', 0, 20, {'tags': 'test-tag'})
    assert results_number == 2


def test_search_dataset_with_format_filter(app, client, search_client, faker):
    for i in range(4):
        title = 'test-{0}'.format(faker.word(ext_word_list=['abc', 'def', 'hij', 'klm', 'nop', 'qrs', 'tuv']))
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
            views=faker.random_int(),
            followers=faker.random_int(),
            reuses=faker.random_int(),
            featured=faker.random_int(),
            resources_count=faker.random_int(min=1, max=15),
            concat_title_org=title + ' ' + acronym + ' ' + organization,
            organization=faker.md5(),
            description=faker.sentence(nb_words=10),
            organization_name=organization,
            format=['pdf'] if i % 2 else ['csv'],
            frequency=faker.word(),
        ))

    # Without this, ElasticSearch does not seem to have the time to index.
    time.sleep(2)

    results_number, res = search_client.query_datasets('test', 0, 20, {})
    assert results_number == 4
    results_number, res = search_client.query_datasets('test', 0, 20, {'format': 'pdf'})
    assert results_number == 2


def test_search_dataset_with_license_filter(app, client, search_client, faker):
    for i in range(4):
        title = 'test-{0}'.format(faker.word(ext_word_list=['abc', 'def', 'hij', 'klm', 'nop', 'qrs', 'tuv']))
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
            views=faker.random_int(),
            followers=faker.random_int(),
            reuses=faker.random_int(),
            featured=faker.random_int(),
            resources_count=faker.random_int(min=1, max=15),
            concat_title_org=title + ' ' + acronym + ' ' + organization,
            organization=faker.md5(),
            description=faker.sentence(nb_words=10),
            organization_name=organization,
            format=['pdf'],
            frequency=faker.word(),
            license='cc-by' if i % 2 else 'fr-lo'
        ))

    # Without this, ElasticSearch does not seem to have the time to index.
    time.sleep(2)

    results_number, res = search_client.query_datasets('test', 0, 20, {})
    assert results_number == 4
    results_number, res = search_client.query_datasets('test', 0, 20, {'license': 'cc-by'})
    assert results_number == 2


def test_search_dataset_with_geozone_filter(app, client, search_client, faker):
    for i in range(4):
        title = 'test-{0}'.format(faker.word(ext_word_list=['abc', 'def', 'hij', 'klm', 'nop', 'qrs', 'tuv']))
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
            views=faker.random_int(),
            followers=faker.random_int(),
            reuses=faker.random_int(),
            featured=faker.random_int(),
            resources_count=faker.random_int(min=1, max=15),
            concat_title_org=title + ' ' + acronym + ' ' + organization,
            organization=faker.md5(),
            description=faker.sentence(nb_words=10),
            organization_name=organization,
            format=['pdf'],
            frequency=faker.word(),
            geozone='country:fr' if i % 2 else 'country:ro'
        ))

    # Without this, ElasticSearch does not seem to have the time to index.
    time.sleep(2)

    results_number, res = search_client.query_datasets('test', 0, 20, {})
    assert results_number == 4
    results_number, res = search_client.query_datasets('test', 0, 20, {'geozone': 'country:fr'})
    assert results_number == 2


def test_search_dataset_with_granularity_filter(app, client, search_client, faker):
    for i in range(4):
        title = 'test-{0}'.format(faker.word(ext_word_list=['abc', 'def', 'hij', 'klm', 'nop', 'qrs', 'tuv']))
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
            views=faker.random_int(),
            followers=faker.random_int(),
            reuses=faker.random_int(),
            featured=faker.random_int(),
            resources_count=faker.random_int(min=1, max=15),
            concat_title_org=title + ' ' + acronym + ' ' + organization,
            organization=faker.md5(),
            description=faker.sentence(nb_words=10),
            organization_name=organization,
            format=['pdf'],
            frequency=faker.word(),
            granularity='country' if i % 2 else 'country-subset'
        ))

    # Without this, ElasticSearch does not seem to have the time to index.
    time.sleep(2)

    results_number, res = search_client.query_datasets('test', 0, 20, {})
    assert results_number == 4
    results_number, res = search_client.query_datasets('test', 0, 20, {'granularity': 'country'})
    assert results_number == 2


def test_search_reuse_with_type_filter(app, client, search_client, faker):
    for i in range(4):
        organization = 'test-{0}'.format(faker.company())
        search_client.index_reuse(Reuse(
            id=faker.md5(),
            title='test-{0}'.format(i) if i % 2 else faker.word(ext_word_list=['abc', 'def', 'hij', 'klm', 'nop', 'qrs', 'tuv']),
            url=faker.uri(),
            created_at=faker.date(),
            orga_followers=faker.random_int(),
            views=faker.random_int(),
            followers=faker.random_int(),
            datasets=faker.random_int(),
            featured=faker.random_int(),
            organization=faker.md5(),
            description=faker.sentence(nb_words=10),
            organization_name=organization,
            type='api' if i % 2 else 'application',
            topic=faker.word()
        ))

    # Without this, ElasticSearch does not seem to have the time to index.
    time.sleep(2)

    results_number, res = search_client.query_reuses('test', 0, 20, {})
    assert results_number == 4
    results_number, res = search_client.query_reuses('test', 0, 20, {'type': 'api'})
    assert results_number == 2


def test_search_reuse_with_topic_filter(app, client, search_client, faker):
    for i in range(4):
        organization = 'test-{0}'.format(faker.company())
        search_client.index_reuse(Reuse(
            id=faker.md5(),
            title='test-{0}'.format(i) if i % 2 else faker.word(ext_word_list=['abc', 'def', 'hij', 'klm', 'nop', 'qrs', 'tuv']),
            url=faker.uri(),
            created_at=faker.date(),
            orga_followers=faker.random_int(),
            views=faker.random_int(),
            followers=faker.random_int(),
            datasets=faker.random_int(),
            featured=faker.random_int(),
            organization=faker.md5(),
            description=faker.sentence(nb_words=10),
            organization_name=organization,
            type=faker.word(),
            topic='transport_and_mobility' if i % 2 else 'housing_and_development'
        ))

    # Without this, ElasticSearch does not seem to have the time to index.
    time.sleep(2)

    results_number, res = search_client.query_reuses('test', 0, 20, {})
    assert results_number == 4
    results_number, res = search_client.query_reuses('test', 0, 20, {'topic': 'transport_and_mobility'})
    assert results_number == 2