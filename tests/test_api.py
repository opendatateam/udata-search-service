import datetime
import time
from flask import url_for

from app.domain.entities import Dataset, Organization, Reuse


def test_api_search_without_query(app, client, search_client, faker):
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
        title = 'test-{0}'.format(i) if i % 2 else 'not-{0}'.format(i)
        acronym = faker.company_suffix()
        organization = 'test-{0}'.format(faker.company()) if i % 2 else 'not-{0}'.format(faker.company())
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
            title='test-{0}'.format(i) if i % 2 else 'not-{0}'.format(i),
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

    resp = client.get(url_for('api.dataset_search'))
    assert resp.json['total'] == 4
    resp = client.get(url_for('api.organization_search'))
    assert resp.json['total'] == 4
    resp = client.get(url_for('api.reuse_search'))
    assert resp.json['total'] == 4

    resp = client.get(url_for('api.dataset_search', q='test'))
    assert resp.json['total'] == 2
    resp = client.get(url_for('api.organization_search', q='test'))
    assert resp.json['total'] == 2
    resp = client.get(url_for('api.reuse_search',  q='test'))
    assert resp.json['total'] == 2


def test_api_dataset_search_with_temporal_filter(app, client, search_client, faker):
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

    resp = client.get(url_for('api.dataset_search', temporal_coverage='2020-02-25-2020-03-10'))
    assert resp.json['total'] == 2

    resp = client.get(url_for('api.dataset_search', temporal_coverage='2020-02-25-20111111111111-10'))
    assert resp.status_code == 400
