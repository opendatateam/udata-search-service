import datetime
import time
from flask import url_for

from udata_search_service.domain.factories import DatasetFactory, OrganizationFactory, ReuseFactory


def test_api_search_without_query(app, client, search_client, faker):
    for i in range(4):
        search_client.index_dataset(DatasetFactory())
        search_client.index_organization(OrganizationFactory())
        search_client.index_reuse(ReuseFactory())

    # Without this, ElasticSearch does not seem to have the time to index.
    time.sleep(2)

    dataset_resp = client.get(url_for('api.dataset_search'))
    assert len(dataset_resp.json['data']) == 4
    assert dataset_resp.json['next_page'] is None
    assert dataset_resp.json['page'] == 1
    assert dataset_resp.json['previous_page'] is None
    assert dataset_resp.json['page_size'] == 20
    assert dataset_resp.json['total_pages'] == 1
    assert dataset_resp.json['total'] == 4

    org_resp = client.get(url_for('api.organization_search'))
    assert len(org_resp.json['data']) == 4
    assert org_resp.json['next_page'] is None
    assert org_resp.json['page'] == 1
    assert org_resp.json['previous_page'] is None
    assert org_resp.json['page_size'] == 20
    assert org_resp.json['total_pages'] == 1
    assert org_resp.json['total'] == 4

    reuse_resp = client.get(url_for('api.reuse_search'))
    assert len(reuse_resp.json['data']) == 4
    assert reuse_resp.json['next_page'] is None
    assert reuse_resp.json['page'] == 1
    assert reuse_resp.json['previous_page'] is None
    assert reuse_resp.json['page_size'] == 20
    assert reuse_resp.json['total_pages'] == 1
    assert reuse_resp.json['total'] == 4


def test_api_search_pagination_without_query(app, client, search_client, faker):
    for i in range(4):
        search_client.index_dataset(DatasetFactory())
        search_client.index_organization(OrganizationFactory())
        search_client.index_reuse(ReuseFactory())

    # Without this, ElasticSearch does not seem to have the time to index.
    time.sleep(2)

    # Test next_url

    dataset_resp = client.get(url_for('api.dataset_search', page_size=2, page=1))
    assert len(dataset_resp.json['data']) == 2
    assert '/api/1/datasets/?page=2&page_size=2' in dataset_resp.json['next_page']
    assert dataset_resp.json['page'] == 1
    assert dataset_resp.json['previous_page'] is None
    assert dataset_resp.json['page_size'] == 2
    assert dataset_resp.json['total_pages'] == 2
    assert dataset_resp.json['total'] == 4

    org_resp = client.get(url_for('api.organization_search', page_size=2, page=1))
    assert len(org_resp.json['data']) == 2
    assert '/api/1/organizations/?page=2&page_size=2' in org_resp.json['next_page']
    assert org_resp.json['page'] == 1
    assert org_resp.json['previous_page'] is None
    assert org_resp.json['page_size'] == 2
    assert org_resp.json['total_pages'] == 2
    assert org_resp.json['total'] == 4

    reuse_resp = client.get(url_for('api.reuse_search', page_size=2, page=1))
    assert len(reuse_resp.json['data']) == 2
    assert '/api/1/reuses/?page=2&page_size=2' in reuse_resp.json['next_page']
    assert reuse_resp.json['page'] == 1
    assert reuse_resp.json['previous_page'] is None
    assert reuse_resp.json['page_size'] == 2
    assert reuse_resp.json['total_pages'] == 2
    assert reuse_resp.json['total'] == 4

    # Test previous_url

    dataset_resp = client.get(url_for('api.dataset_search', page_size=2, page=2))
    assert len(dataset_resp.json['data']) == 2
    assert dataset_resp.json['next_page'] is None
    assert dataset_resp.json['page'] == 2
    assert '/api/1/datasets/?page=1&page_size=2' in dataset_resp.json['previous_page']
    assert dataset_resp.json['page_size'] == 2
    assert dataset_resp.json['total_pages'] == 2
    assert dataset_resp.json['total'] == 4

    org_resp = client.get(url_for('api.organization_search', page_size=2, page=2))
    assert len(org_resp.json['data']) == 2
    assert org_resp.json['next_page'] is None
    assert org_resp.json['page'] == 2
    assert '/api/1/organizations/?page=1&page_size=2' in org_resp.json['previous_page']
    assert org_resp.json['page_size'] == 2
    assert org_resp.json['total_pages'] == 2
    assert org_resp.json['total'] == 4

    reuse_resp = client.get(url_for('api.reuse_search', page_size=2, page=2))
    assert len(reuse_resp.json['data']) == 2
    assert reuse_resp.json['page'] == 2
    assert reuse_resp.json['next_page'] is None
    assert '/api/1/reuses/?page=1&page_size=2' in reuse_resp.json['previous_page']
    assert reuse_resp.json['page_size'] == 2
    assert reuse_resp.json['total_pages'] == 2
    assert reuse_resp.json['total'] == 4


def test_api_search_with_query(app, client, search_client, faker):
    for i in range(4):
        title = 'sample-test-{0}'.format(i) if i % 2 else i
        organization = 'sample-test-{0}'.format(faker.company()) if i % 2 else 'not-{0}'.format(faker.company())
        search_client.index_dataset(DatasetFactory(title=title))
        search_client.index_organization(OrganizationFactory(name=organization))
        search_client.index_reuse(ReuseFactory(title='sample-test-{0}'.format(i) if i % 2 else 'not-{0}'.format(i)))

    # Without this, ElasticSearch does not seem to have the time to index.
    time.sleep(2)

    resp = client.get(url_for('api.dataset_search'))
    assert resp.json['total'] == 4
    resp = client.get(url_for('api.organization_search'))
    assert resp.json['total'] == 4
    resp = client.get(url_for('api.reuse_search'))
    assert resp.json['total'] == 4

    resp = client.get(url_for('api.dataset_search', q='sample-test'))
    assert resp.json['total'] == 2
    resp = client.get(url_for('api.organization_search', q='sample-test'))
    assert resp.json['total'] == 2
    resp = client.get(url_for('api.reuse_search',  q='sample-test'))
    assert resp.json['total'] == 2


def test_api_dataset_search_with_temporal_filter(app, client, search_client, faker):
    for i in range(4):
        search_client.index_dataset(DatasetFactory(
            temporal_coverage_start=datetime.date(2021, 12, 2) if i % 2 else datetime.date(2020, 2, 24),
            temporal_coverage_end=datetime.date(2022, 1, 1) if i % 2 else datetime.date(2022, 2, 13)
        ))
    # Without this, ElasticSearch does not seem to have the time to index.
    time.sleep(2)

    resp = client.get(url_for('api.dataset_search', temporal_coverage='2020-02-25-2020-03-10'))
    assert resp.json['total'] == 2

    resp = client.get(url_for('api.dataset_search', temporal_coverage='2020-02-25-20111111111111-10'))
    assert resp.status_code == 400


def test_api_search_with_tag_filter(app, client, search_client, faker):
    for i in range(4):
        search_client.index_dataset(DatasetFactory(tags=['test-tag'] if i % 2 else ['not-test-tag']))
        search_client.index_reuse(ReuseFactory(tags=['test-tag'] if i % 2 else ['not-test-tag']))

    # Without this, ElasticSearch does not seem to have the time to index.
    time.sleep(2)

    # Filter is singular, we test the feature that maps it to the plural field of the entity.
    resp = client.get(url_for('api.dataset_search'))
    assert resp.json['total'] == 4

    resp = client.get(url_for('api.dataset_search', tag='test-tag'))
    assert resp.json['total'] == 2

    resp = client.get(url_for('api.reuse_search'))
    assert resp.json['total'] == 4

    resp = client.get(url_for('api.reuse_search', tag='test-tag'))
    assert resp.json['total'] == 2


def test_api_dataset_search_with_geozone_filter(app, client, search_client, faker):
    for i in range(4):
        search_client.index_dataset(DatasetFactory(
            geozones='country:fr' if i % 2 else 'country:ro'
        ))

    # Without this, ElasticSearch does not seem to have the time to index.
    time.sleep(2)

    resp = client.get(url_for('api.dataset_search'))
    assert resp.json['total'] == 4

    # Filter is singular, we test the feature that maps it to the plural field of the entity.
    resp = client.get(url_for('api.dataset_search', geozone='country:fr'))
    assert resp.json['total'] == 2


def test_api_search_with_followers_sorting(app, client, search_client, faker):
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

    # Without this, ElasticSearch does not seem to have the time to index.
    time.sleep(2)

    dataset_resp = client.get(url_for('api.dataset_search', sort='followers'))
    assert len(dataset_resp.json['data']) == 2
    assert dataset_resp.json['data'][0]['title'] == 'data-test-1'
    assert dataset_resp.json['next_page'] is None
    assert dataset_resp.json['page'] == 1
    assert dataset_resp.json['previous_page'] is None
    assert dataset_resp.json['page_size'] == 20
    assert dataset_resp.json['total_pages'] == 1
    assert dataset_resp.json['total'] == 2

    org_resp = client.get(url_for('api.organization_search', sort='followers'))
    assert len(org_resp.json['data']) == 2
    assert org_resp.json['data'][0]['name'] == 'org-test-1'
    assert org_resp.json['next_page'] is None
    assert org_resp.json['page'] == 1
    assert org_resp.json['previous_page'] is None
    assert org_resp.json['page_size'] == 20
    assert org_resp.json['total_pages'] == 1
    assert org_resp.json['total'] == 2

    reuse_resp = client.get(url_for('api.reuse_search', sort='followers'))
    assert len(reuse_resp.json['data']) == 2
    assert reuse_resp.json['data'][0]['title'] == 'reuse-test-1'
    assert reuse_resp.json['next_page'] is None
    assert reuse_resp.json['page'] == 1
    assert reuse_resp.json['previous_page'] is None
    assert reuse_resp.json['page_size'] == 20
    assert reuse_resp.json['total_pages'] == 1
    assert reuse_resp.json['total'] == 2


def test_api_search_with_followers_decreasing_sorting(app, client, search_client, faker):
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

    # Without this, ElasticSearch does not seem to have the time to index.
    time.sleep(2)

    dataset_resp = client.get(url_for('api.dataset_search', sort='-followers'))
    assert len(dataset_resp.json['data']) == 2
    assert dataset_resp.json['data'][0]['title'] == 'data-test-2'
    assert dataset_resp.json['next_page'] is None
    assert dataset_resp.json['page'] == 1
    assert dataset_resp.json['previous_page'] is None
    assert dataset_resp.json['page_size'] == 20
    assert dataset_resp.json['total_pages'] == 1
    assert dataset_resp.json['total'] == 2

    org_resp = client.get(url_for('api.organization_search', sort='-followers'))
    assert len(org_resp.json['data']) == 2
    assert org_resp.json['data'][0]['name'] == 'org-test-2'
    assert org_resp.json['next_page'] is None
    assert org_resp.json['page'] == 1
    assert org_resp.json['previous_page'] is None
    assert org_resp.json['page_size'] == 20
    assert org_resp.json['total_pages'] == 1
    assert org_resp.json['total'] == 2

    reuse_resp = client.get(url_for('api.reuse_search', sort='-followers'))
    assert len(reuse_resp.json['data']) == 2
    assert reuse_resp.json['data'][0]['title'] == 'reuse-test-2'
    assert reuse_resp.json['next_page'] is None
    assert reuse_resp.json['page'] == 1
    assert reuse_resp.json['previous_page'] is None
    assert reuse_resp.json['page_size'] == 20
    assert reuse_resp.json['total_pages'] == 1
    assert reuse_resp.json['total'] == 2
