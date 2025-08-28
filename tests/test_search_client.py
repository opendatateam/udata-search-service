import datetime
import time

from udata_search_service.domain.factories import DataserviceFactory, DatasetFactory, OrganizationFactory, ReuseFactory

ext_word_list = ['abc', 'def', 'hij', 'klm', 'nop', 'qrs', 'tuv']


def test_general_search_with_and_without_query(app, client, search_client, faker):
    for i in range(4):
        search_client.index_dataset(DatasetFactory(
            title='test-{0}'.format(i) if i % 2 else faker.word(ext_word_list=ext_word_list),
            description='udata' if i == 1 else faker.word()
        ))
        search_client.index_organization(OrganizationFactory(
            name='test-{0}'.format(faker.company()) if i % 2 else faker.company(),
            description='udata' if i == 1 else faker.word()
        ))
        search_client.index_reuse(ReuseFactory(
            title='test-{0}'.format(i) if i % 2 else faker.word(ext_word_list=ext_word_list),
            description='udata' if i == 1 else faker.word()
        ))
        search_client.index_dataservice(DataserviceFactory(
            title='test-{0}'.format(i) if i % 2 else faker.word(ext_word_list=ext_word_list),
            description='udata' if i == 1 else faker.word()
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
    results_number, res = search_client.query_dataservices('test', 0, 20, {})
    assert results_number == 2

    # Should return only the object with string 'test' and 'udata' in their titles/names or desc
    results_number, res = search_client.query_datasets('test udata', 0, 20, {})
    assert results_number == 1
    results_number, res = search_client.query_organizations('test udata', 0, 20, {})
    assert results_number == 1
    results_number, res = search_client.query_reuses('test udata', 0, 20, {})
    assert results_number == 1
    results_number, res = search_client.query_dataservices('test udata', 0, 20, {})
    assert results_number == 1

    # Should return all objects, as query text is none
    results_number, res = search_client.query_datasets(None, 0, 20, {})
    assert results_number == 4
    results_number, res = search_client.query_organizations(None, 0, 20, {})
    assert results_number == 4
    results_number, res = search_client.query_reuses(None, 0, 20, {})
    assert results_number == 4
    results_number, res = search_client.query_dataservices(None, 0, 20, {})
    assert results_number == 4


def test_search_with_orga_sp_filter(app, client, search_client, faker):
    for i in range(4):
        search_client.index_dataset(DatasetFactory(
            orga_sp=4 if i % 2 else 1
        ))
        search_client.index_organization(OrganizationFactory(
            orga_sp=4 if i % 2 else 1
        ))

    # Without this, ElasticSearch does not seem to have the time to index.
    time.sleep(2)

    results_number, res = search_client.query_datasets(None, 0, 20, {})
    assert results_number == 4
    results_number, res = search_client.query_datasets(None, 0, 20, {'orga_sp': 4})
    assert results_number == 2
    results_number, res = search_client.query_organizations(None, 0, 20, {})
    assert results_number == 4
    results_number, res = search_client.query_organizations(None, 0, 20, {'orga_sp': 4})
    assert results_number == 2


def test_search_with_orga_id_filter(app, client, search_client, faker):
    for i in range(4):
        search_client.index_dataset(DatasetFactory())
        search_client.index_reuse(ReuseFactory())

    search_client.index_dataset(DatasetFactory(
        organization='64f01c248bf99eab7c197717'
    ))
    search_client.index_reuse(ReuseFactory(
        organization='77f01c346bf99eab7c198891'
    ))
    search_client.index_dataservice(DataserviceFactory(
        organization='77f01c346bf99eab7c198891'
    ))

    # Without this, ElasticSearch does not seem to have the time to index.
    time.sleep(2)

    results_number, res = search_client.query_datasets(None, 0, 20, {})
    assert results_number == 5
    results_number, res = search_client.query_datasets(None, 0, 20, {'organization': '64f01c248bf99eab7c197717'})
    assert results_number == 1
    results_number, res = search_client.query_reuses(None, 0, 20, {})
    assert results_number == 5
    results_number, res = search_client.query_reuses(None, 0, 20, {'organization': '77f01c346bf99eab7c198891'})
    assert results_number == 1
    results_number, res = search_client.query_dataservices(None, 0, 20, {'organization': '77f01c346bf99eab7c198891'})
    assert results_number == 1


def test_search_with_orga_badge_filter(app, client, search_client, faker):
    for i in range(4):
        search_client.index_dataset(DatasetFactory(
            organization_badges=["public-service"] if i % 2 else []
        ))
        search_client.index_reuse(ReuseFactory(
            organization_badges=["public-service"] if i % 2 else []
        ))

    # Without this, ElasticSearch does not seem to have the time to index.
    time.sleep(2)

    results_number, res = search_client.query_datasets(None, 0, 20, {})
    assert results_number == 4
    results_number, res = search_client.query_datasets(None, 0, 20, {'organization_badges': 'public-service'})
    assert results_number == 2
    results_number, res = search_client.query_reuses(None, 0, 20, {})
    assert results_number == 4
    results_number, res = search_client.query_reuses(None, 0, 20, {'organization_badges': 'public-service'})
    assert results_number == 2


def test_search_with_owner_filter(app, client, search_client, faker):
    for i in range(4):
        search_client.index_dataset(DatasetFactory())
        search_client.index_reuse(ReuseFactory())

    search_client.index_dataset(DatasetFactory(
        owner='64f01c248bf99eab7c197717'
    ))

    search_client.index_reuse(ReuseFactory(
        owner='77f01c346bf99eab7c198891'
    ))

    search_client.index_dataservice(DataserviceFactory(
        owner='77f01c346bf99eab7c198891'
    ))

    # Without this, ElasticSearch does not seem to have the time to index.
    time.sleep(2)

    results_number, res = search_client.query_datasets(None, 0, 20, {})
    assert results_number == 5
    results_number, res = search_client.query_datasets(None, 0, 20, {'owner': '64f01c248bf99eab7c197717'})
    assert results_number == 1
    results_number, res = search_client.query_reuses(None, 0, 20, {})
    assert results_number == 5
    results_number, res = search_client.query_reuses(None, 0, 20, {'owner': '77f01c346bf99eab7c198891'})
    assert results_number == 1
    results_number, res = search_client.query_dataservices(None, 0, 20, {'owner': '77f01c346bf99eab7c198891'})
    assert results_number == 1


def test_search_datasets_with_temporal_filters(app, client, search_client, faker):
    for i in range(4):
        search_client.index_dataset(DatasetFactory(
            temporal_coverage_start=datetime.date(2021, 12, 2) if i % 2 else datetime.date(2020, 2, 24),
            temporal_coverage_end=datetime.date(2022, 1, 1) if i % 2 else datetime.date(2022, 2, 13)
        ))
    # Without this, ElasticSearch does not seem to have the time to index.
    time.sleep(2)

    results_number, res = search_client.query_datasets(None, 0, 20, {})
    assert results_number == 4
    results_number, res = search_client.query_datasets(None, 0, 20, {
        'temporal_coverage_start': datetime.date(2020, 2, 25),
        'temporal_coverage_end': datetime.date(2020, 3, 10)
    })
    assert results_number == 2
    results_number, res = search_client.query_datasets(None, 0, 20, {
        'temporal_coverage_start': datetime.date(2019, 2, 25),
        'temporal_coverage_end': datetime.date(2020, 3, 10)
    })
    assert results_number == 0


def test_search_with_tag_filter(app, client, search_client, faker):
    for i in range(4):
        search_client.index_dataset(DatasetFactory(
            tags=['test-tag', f'test-tag-{i}'] if i % 2 else ['not-test-tag']
        ))

        search_client.index_reuse(ReuseFactory(
            tags=['test-tag'] if i % 2 else ['not-test-tag']
        ))

        search_client.index_dataservice(DataserviceFactory(
            tags=['test-tag'] if i % 2 else ['not-test-tag']
        ))

    # Without this, ElasticSearch does not seem to have the time to index.
    time.sleep(2)

    results_number, res = search_client.query_datasets(None, 0, 20, {})
    assert results_number == 4
    results_number, res = search_client.query_datasets(None, 0, 20, {'tags': ['test-tag', 'test-tag-1']})
    assert results_number == 1
    results_number, res = search_client.query_datasets(None, 0, 20, {'tags': ['not-test-tag']})
    assert results_number == 2
    results_number, res = search_client.query_reuses(None, 0, 20, {})
    assert results_number == 4
    results_number, res = search_client.query_reuses(None, 0, 20, {'tags': 'test-tag'})
    assert results_number == 2
    results_number, res = search_client.query_dataservices(None, 0, 20, {'tags': 'test-tag'})
    assert results_number == 2


def test_search_dataset_with_format_filter(app, client, search_client, faker):
    for i in range(4):
        search_client.index_dataset(DatasetFactory(
            format=['pdf'] if i % 2 else ['csv']
        ))

    # Without this, ElasticSearch does not seem to have the time to index.
    time.sleep(2)

    results_number, res = search_client.query_datasets(None, 0, 20, {})
    assert results_number == 4
    results_number, res = search_client.query_datasets(None, 0, 20, {'format': 'pdf'})
    assert results_number == 2


def test_search_dataset_with_license_filter(app, client, search_client, faker):
    for i in range(4):
        search_client.index_dataset(DatasetFactory(
            license='cc-by' if i % 2 else 'fr-lo'
        ))

    # Without this, ElasticSearch does not seem to have the time to index.
    time.sleep(2)

    results_number, res = search_client.query_datasets(None, 0, 20, {})
    assert results_number == 4
    results_number, res = search_client.query_datasets(None, 0, 20, {'license': 'cc-by'})
    assert results_number == 2


def test_search_dataset_with_geozone_filter(app, client, search_client, faker):
    for i in range(4):
        search_client.index_dataset(DatasetFactory(
            geozones='country:fr' if i % 2 else 'country:ro'
        ))

    # Without this, ElasticSearch does not seem to have the time to index.
    time.sleep(2)

    results_number, res = search_client.query_datasets(None, 0, 20, {})
    assert results_number == 4
    results_number, res = search_client.query_datasets(None, 0, 20, {'geozones': 'country:fr'})
    assert results_number == 2


def test_search_dataset_with_granularity_filter(app, client, search_client, faker):
    for i in range(4):
        search_client.index_dataset(DatasetFactory(
            granularity='country' if i % 2 else 'country-subset'
        ))

    # Without this, ElasticSearch does not seem to have the time to index.
    time.sleep(2)

    results_number, res = search_client.query_datasets(None, 0, 20, {})
    assert results_number == 4
    results_number, res = search_client.query_datasets(None, 0, 20, {'granularity': 'country'})
    assert results_number == 2


def test_search_dataset_with_schema_filter(app, client, search_client, faker):
    for i in range(4):
        search_client.index_dataset(DatasetFactory(
            title='data-test-1',
            schema=['etalab/schema-irve'] if i % 2 else []
        ))

    # Without this, ElasticSearch does not seem to have the time to index.
    time.sleep(2)

    results_number, res = search_client.query_datasets('test', 0, 20, {})
    assert results_number == 4
    results_number, res = search_client.query_datasets('test', 0, 20, {'schema': 'etalab/schema-irve'})
    assert results_number == 2


def test_search_reuse_with_type_filter(app, client, search_client, faker):
    for i in range(4):
        search_client.index_reuse(ReuseFactory(
            type='api' if i % 2 else 'application'
        ))

    # Without this, ElasticSearch does not seem to have the time to index.
    time.sleep(2)

    results_number, res = search_client.query_reuses(None, 0, 20, {})
    assert results_number == 4
    results_number, res = search_client.query_reuses(None, 0, 20, {'type': 'api'})
    assert results_number == 2


def test_search_reuse_with_topic_filter(app, client, search_client, faker):
    for i in range(4):
        search_client.index_reuse(ReuseFactory(
            topic='transport_and_mobility' if i % 2 else 'housing_and_development'
        ))

    # Without this, ElasticSearch does not seem to have the time to index.
    time.sleep(2)

    results_number, res = search_client.query_reuses(None, 0, 20, {})
    assert results_number == 4
    results_number, res = search_client.query_reuses(None, 0, 20, {'topic': 'transport_and_mobility'})
    assert results_number == 2


def test_search_dataservice_with_is_restricted_filter(app, client, search_client, faker):
    for i in range(4):
        search_client.index_dataservice(DataserviceFactory(
            is_restricted=bool(i % 2)
        ))

    # Without this, ElasticSearch does not seem to have the time to index.
    time.sleep(2)

    results_number, res = search_client.query_dataservices(None, 0, 20, {})
    assert results_number == 4
    results_number, res = search_client.query_dataservices(None, 0, 20, {'is_restricted': True})
    assert results_number == 2
    results_number, res = search_client.query_dataservices(None, 0, 20, {'is_restricted': False})
    assert results_number == 2


def test_general_search_with_sorting(app, client, search_client, faker):
    search_client.index_dataset(DatasetFactory(
        title='data-test-1',
        followers=0
    ))
    search_client.index_dataset(DatasetFactory(
        title='data-test-2',
        followers=3
    ))
    search_client.index_organization(OrganizationFactory(
        name='org-test-1',
        followers=0
    ))
    search_client.index_organization(OrganizationFactory(
        name='org-test-2',
        followers=3
    ))
    search_client.index_reuse(ReuseFactory(
        title='reuse-test-1',
        followers=0
    ))
    search_client.index_reuse(ReuseFactory(
        title='reuse-test-2',
        followers=3
    ))
    search_client.index_dataservice(DataserviceFactory(
        title='dataservice-test-1',
        followers=0
    ))
    search_client.index_dataservice(DataserviceFactory(
        title='dataservice-test-2',
        followers=3
    ))
    # Without this, ElasticSearch does not seem to have the time to index.
    time.sleep(2)

    # Sort ascending
    results_number, res = search_client.query_datasets(None, 0, 20, {}, sort='followers')
    assert res[0]['title'] == 'data-test-1'
    results_number, res = search_client.query_organizations(None, 0, 20, {}, sort='followers')
    assert res[0]['name'] == 'org-test-1'
    results_number, res = search_client.query_reuses(None, 0, 20, {}, sort='followers')
    assert res[0]['title'] == 'reuse-test-1'
    results_number, res = search_client.query_dataservices(None, 0, 20, {}, sort='followers')
    assert res[0]['title'] == 'dataservice-test-1'

    # Sort descending
    results_number, res = search_client.query_datasets(None, 0, 20, {}, sort='-followers')
    assert res[0]['title'] == 'data-test-2'
    results_number, res = search_client.query_organizations(None, 0, 20, {}, sort='-followers')
    assert res[0]['name'] == 'org-test-2'
    results_number, res = search_client.query_reuses(None, 0, 20, {}, sort='-followers')
    assert res[0]['title'] == 'reuse-test-2'
    results_number, res = search_client.query_dataservices(None, 0, 20, {}, sort='-followers')
    assert res[0]['title'] == 'dataservice-test-2'

def test_search_reuse_with_score_functions(app, client, search_client, faker):
    # Create generic reuses without bonus/malus
    for i in range(8):
        search_client.index_reuse(ReuseFactory(archived=None, views=0))
    # Create specific reuse with script score malus field
    search_client.index_reuse(ReuseFactory(
        archived=datetime.datetime.utcnow(),
        views=0
    ))
    # Create specific reuse with field value factor bonus field
    search_client.index_reuse(ReuseFactory(
        archived=None,
        views=100000
    ))

    # Without this, ElasticSearch does not seem to have the time to index.
    time.sleep(2)

    results_number, res = search_client.query_reuses(None, 0, 20, {})
    assert results_number == 10
    assert res[0]["views"] == 100000
    assert res[-1]["archived"] is not None

def test_general_search_with_sorting_last_update(app, client, search_client, faker):
    search_client.index_dataset(DatasetFactory(
        title='data-test-1',
        last_update=datetime.date(2021, 12, 2)
    ))
    search_client.index_dataset(DatasetFactory(
        title='data-test-2',
        last_update=datetime.date(2022, 12, 2)
    ))
    search_client.index_dataset(DatasetFactory(
        title='data-test-3',
        last_update=datetime.date(2023, 12, 2)
    ))
    # Without this, ElasticSearch does not seem to have the time to index.
    time.sleep(2)

    results_number, res = search_client.query_datasets(None, 0, 20, {}, sort='-last_update')
    assert res[0]['title'] == 'data-test-3'
    results_number, res = search_client.query_datasets(None, 0, 20, {}, sort='last_update')
    assert res[0]['title'] == 'data-test-1'


def test_search_dataset_with_synonym(app, client, search_client, faker):
    search_client.index_dataset(DatasetFactory(
        title='rp',
    ))

    # Without this, ElasticSearch does not seem to have the time to index.
    time.sleep(2)

    results_number, res = search_client.query_datasets('recensement population', 0, 20, {})
    assert results_number == 1


def test_search_dataset_by_id(app, client, search_client, faker):
    dataset = DatasetFactory()
    search_client.index_dataset(dataset)

    # Without this, ElasticSearch does not seem to have the time to index.
    time.sleep(2)

    results_number, res = search_client.query_datasets(str(dataset.id), 0, 20, {})
    assert results_number == 1


def test_search_dataset_by_resource_title_and_id(app, client, search_client, faker):
    dataset = DatasetFactory()
    search_client.index_dataset(dataset)

    for i in range(4):
        search_client.index_dataset(DatasetFactory())

    # Without this, ElasticSearch does not seem to have the time to index.
    time.sleep(2)

    results_number, res = search_client.query_datasets(str(dataset.resources_ids[0]), 0, 20, {})
    assert results_number == 1

    results_number, res = search_client.query_datasets(str(dataset.resources_titles[0]), 0, 20, {})
    assert results_number == 1

