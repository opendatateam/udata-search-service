import datetime
import time
from flask import url_for

from udata_search_service.domain.factories import DatasetFactory, OrganizationFactory, ReuseFactory


def test_api_dataset_index_unindex(app, client, faker):
    dataset = {
        'id': faker.md5(),
        'title': faker.sentence(),
        'description': faker.text(),
        'acronym': faker.company_suffix(),
        'url': faker.url(),
        'created_at': faker.past_datetime().isoformat(),
        'last_update': faker.past_datetime().isoformat(),
        'views': faker.random_int(),
        'followers': faker.random_int(),
        'reuses': faker.random_int(),
        'featured': faker.random_int(min=0, max=1),
        'resources_count': faker.random_int(min=1, max=15),
        'resources': [
            {"id": faker.md5(), "title": faker.word()}
            for _ in range(5)
        ],
        'organization': {
            'id': faker.md5(),
            'name': faker.company(),
            'public_service': faker.random_int(min=0, max=1),
            'followers': faker.random_int(),
            'badges': [faker.word()]
        },
        'format': ['pdf'],
        'frequency': 'unknown',
        'badges': [],
        'tags': [faker.word()],
        'license': faker.word(),
        'temporal_coverage_start': faker.past_datetime().isoformat(),
        'temporal_coverage_end': faker.past_datetime().isoformat(),
        'granularity': faker.word(),
        'geozones': [{'id': faker.word(), 'name': faker.word(), 'keys': [faker.random_int()]},
                     {'id': faker.word()}],
        'owner': None,
        'extras': {},
        'harvest': {},
        'schema': [faker.word(), faker.word()]
    }

    query = {
        'document': dataset,
        'index': None
    }

    index_resp = client.post(url_for('api.dataset_index'), json={'document': dataset, 'index': 'random-non-existing-index'})
    assert index_resp.status_code == 404

    index_resp = client.post(url_for('api.dataset_index'), json=query)
    assert index_resp.status_code == 200

    time.sleep(2)

    dataset_resp = client.get(url_for('api.dataset_get_specific', dataset_id=dataset['id']))
    assert dataset_resp.status_code == 200
    assert dataset_resp.json['title'] == dataset['title']

    dataset_search_resp = client.get(url_for('api.dataset_search'))
    assert len(dataset_search_resp.json['data']) == 1
    assert dataset_search_resp.json['data'][0]['title'] == dataset['title']
    assert dataset_search_resp.json['next_page'] is None
    assert dataset_search_resp.json['page'] == 1
    assert dataset_search_resp.json['previous_page'] is None
    assert dataset_search_resp.json['page_size'] == 20
    assert dataset_search_resp.json['total_pages'] == 1
    assert dataset_search_resp.json['total'] == 1

    deletion_resp = client.delete(url_for('api.dataset_unindex', dataset_id=dataset['id']))
    assert deletion_resp.status_code == 200

    time.sleep(2)

    dataset_get_after_delete_resp = client.get(url_for('api.dataset_get_specific', dataset_id=dataset['id']))
    assert dataset_get_after_delete_resp.status_code == 404

    dataset_search_after_delete_resp = client.get(url_for('api.dataset_search'))
    assert len(dataset_search_after_delete_resp.json['data']) == 0
    assert dataset_search_after_delete_resp.json['next_page'] is None
    assert dataset_search_after_delete_resp.json['page'] == 1
    assert dataset_search_after_delete_resp.json['previous_page'] is None
    assert dataset_search_after_delete_resp.json['page_size'] == 20
    assert dataset_search_after_delete_resp.json['total_pages'] == 1
    assert dataset_search_after_delete_resp.json['total'] == 0


def test_api_dataset_index_on_another_index(app, client, search_client, faker):
    now = datetime.datetime.utcnow().strftime('%Y-%m-%d-%H-%M')
    index_name = f"test-dataset-{now}"
    if not search_client.es.indices.exists(index=f"{app.config['UDATA_INSTANCE_NAME']}-{index_name}"):
        search_client.es.indices.create(index=f"{app.config['UDATA_INSTANCE_NAME']}-{index_name}")

    dataset = {
        'id': faker.md5(),
        'title': faker.sentence(),
        'description': faker.text(),
        'acronym': faker.company_suffix(),
        'url': faker.url(),
        'created_at': faker.past_datetime().isoformat(),
        'last_update': faker.past_datetime().isoformat(),
        'views': faker.random_int(),
        'followers': faker.random_int(),
        'reuses': faker.random_int(),
        'featured': faker.random_int(min=0, max=1),
        'resources_count': faker.random_int(min=1, max=15),
        'resources': [
            {"id": faker.md5(), "title": faker.word()}
            for _ in range(5)
        ],
        'organization': {
            'id': faker.md5(),
            'name': faker.company(),
            'public_service': faker.random_int(min=0, max=1),
            'followers': faker.random_int(),
            'badges': [faker.word()]
        },
        'format': ['pdf'],
        'frequency': 'unknown',
        'badges': [],
        'tags': [faker.word()],
        'license': faker.word(),
        'temporal_coverage_start': faker.past_datetime().isoformat(),
        'temporal_coverage_end': faker.past_datetime().isoformat(),
        'granularity': faker.word(),
        'geozones': [{'id': faker.word(), 'name': faker.word(), 'keys': [faker.random_int()]},
                     {'id': faker.word()}],
        'owner': None,
        'extras': {},
        'harvest': {},
        'schema': [faker.word(), faker.word()]
    }

    query = {
        'document': dataset,
        'index': index_name
    }

    index_resp = client.post(url_for('api.dataset_index'), json=query)
    assert index_resp.status_code == 200

    time.sleep(2)

    resp = search_client.es.get(index=f"{app.config['UDATA_INSTANCE_NAME']}-{index_name}", id=dataset['id'])
    assert resp['_source']['title'] == dataset['title']


def test_api_org_index_unindex(app, client, faker):
    org = {
        'id': faker.md5(),
        'name': faker.company(),
        'description': faker.text(),
        'url': faker.url(),
        'created_at': faker.past_datetime().isoformat(),
        'views': faker.random_int(),
        'orga_sp': faker.random_int(),
        'followers': faker.random_int(),
        'datasets': faker.random_int(),
        'reuses': faker.random_int(),
        'badges': [],
        'extras': {},
    }

    query = {
        'document': org,
        'index': None
    }

    index_resp = client.post(url_for('api.organization_index'), json={'document': org, 'index': 'random-non-existing-index'})
    assert index_resp.status_code == 404

    index_resp = client.post(url_for('api.organization_index'), json=query)
    assert index_resp.status_code == 200

    time.sleep(2)

    organization_resp = client.get(url_for('api.organization_get_specific', organization_id=org['id']))
    assert organization_resp.status_code == 200
    assert organization_resp.json['name'] == org['name']

    organization_search_resp = client.get(url_for('api.organization_search'))
    assert len(organization_search_resp.json['data']) == 1
    assert organization_search_resp.json['data'][0]['name'] == org['name']
    assert organization_search_resp.json['next_page'] is None
    assert organization_search_resp.json['page'] == 1
    assert organization_search_resp.json['previous_page'] is None
    assert organization_search_resp.json['page_size'] == 20
    assert organization_search_resp.json['total_pages'] == 1
    assert organization_search_resp.json['total'] == 1

    deletion_resp = client.delete(url_for('api.organization_unindex', organization_id=org['id']))
    assert deletion_resp.status_code == 200

    time.sleep(2)

    organization_get_after_delete_resp = client.get(url_for('api.organization_get_specific', organization_id=org['id']))
    assert organization_get_after_delete_resp.status_code == 404

    organization_search_after_delete_resp = client.get(url_for('api.organization_search'))
    assert len(organization_search_after_delete_resp.json['data']) == 0
    assert organization_search_after_delete_resp.json['next_page'] is None
    assert organization_search_after_delete_resp.json['page'] == 1
    assert organization_search_after_delete_resp.json['previous_page'] is None
    assert organization_search_after_delete_resp.json['page_size'] == 20
    assert organization_search_after_delete_resp.json['total_pages'] == 1
    assert organization_search_after_delete_resp.json['total'] == 0


def test_api_org_index_on_another_index(app, client, search_client, faker):
    now = datetime.datetime.utcnow().strftime('%Y-%m-%d-%H-%M')
    index_name = f"test-organization-{now}"
    if not search_client.es.indices.exists(index=f"{app.config['UDATA_INSTANCE_NAME']}-{index_name}"):
        search_client.es.indices.create(index=f"{app.config['UDATA_INSTANCE_NAME']}-{index_name}")

    org = {
        'id': faker.md5(),
        'name': faker.company(),
        'description': faker.text(),
        'url': faker.url(),
        'created_at': faker.past_datetime().isoformat(),
        'views': faker.random_int(),
        'orga_sp': faker.random_int(),
        'followers': faker.random_int(),
        'datasets': faker.random_int(),
        'reuses': faker.random_int(),
        'badges': [],
        'extras': {},
    }

    query = {
        'document': org,
        'index': index_name
    }

    index_resp = client.post(url_for('api.organization_index'), json=query)
    assert index_resp.status_code == 200

    time.sleep(2)

    resp = search_client.es.get(index=f"{app.config['UDATA_INSTANCE_NAME']}-{index_name}", id=org['id'])
    assert resp['_source']['name'] == org['name']


def test_api_reuse_index_unindex(app, client, faker):
    reuse = {
        'id': faker.md5(),
        'title': faker.sentence(),
        'description': faker.text(),
        'url': faker.url(),
        'badges': [],
        'created_at': faker.past_datetime().isoformat(),
        'datasets': faker.random_int(),
        'views': faker.random_int(),
        'followers': faker.random_int(),
        'featured': faker.random_int(min=0, max=1),
        'organization': {
            'id': faker.md5(),
            'name': faker.company(),
            'public_service': faker.random_int(min=0, max=1),
            'followers': faker.random_int(),
            'badges': [faker.word()]
        },
        'tags': [],
        'owner': None,
        'extras': {},
        'type': faker.word(),
        'topic': faker.word()
    }

    query = {
        'document': reuse,
        'index': None
    }

    index_resp = client.post(url_for('api.reuse_index'), json={'document': reuse, 'index': 'random-non-existing-index'})
    assert index_resp.status_code == 404

    index_resp = client.post(url_for('api.reuse_index'), json=query)
    assert index_resp.status_code == 200

    time.sleep(2)

    reuse_resp = client.get(url_for('api.reuse_get_specific', reuse_id=reuse['id']))
    assert reuse_resp.status_code == 200
    assert reuse_resp.json['title'] == reuse['title']

    reuse_search_resp = client.get(url_for('api.reuse_search'))
    assert len(reuse_search_resp.json['data']) == 1
    assert reuse_search_resp.json['data'][0]['title'] == reuse['title']
    assert reuse_search_resp.json['next_page'] is None
    assert reuse_search_resp.json['page'] == 1
    assert reuse_search_resp.json['previous_page'] is None
    assert reuse_search_resp.json['page_size'] == 20
    assert reuse_search_resp.json['total_pages'] == 1
    assert reuse_search_resp.json['total'] == 1

    deletion_resp = client.delete(url_for('api.reuse_unindex', reuse_id=reuse['id']))
    assert deletion_resp.status_code == 200

    time.sleep(2)

    reuse_get_after_delete_resp = client.get(url_for('api.reuse_get_specific', reuse_id=reuse['id']))
    assert reuse_get_after_delete_resp.status_code == 404

    reuse_search_after_delete_resp = client.get(url_for('api.reuse_search'))
    assert len(reuse_search_after_delete_resp.json['data']) == 0
    assert reuse_search_after_delete_resp.json['next_page'] is None
    assert reuse_search_after_delete_resp.json['page'] == 1
    assert reuse_search_after_delete_resp.json['previous_page'] is None
    assert reuse_search_after_delete_resp.json['page_size'] == 20
    assert reuse_search_after_delete_resp.json['total_pages'] == 1
    assert reuse_search_after_delete_resp.json['total'] == 0


def test_api_reuse_index_on_another_index(app, client, search_client, faker):
    now = datetime.datetime.utcnow().strftime('%Y-%m-%d-%H-%M')
    index_name = f"test-reuse-{now}"
    if not search_client.es.indices.exists(index=f"{app.config['UDATA_INSTANCE_NAME']}-{index_name}"):
        search_client.es.indices.create(index=f"{app.config['UDATA_INSTANCE_NAME']}-{index_name}")

    reuse = {
        'id': faker.md5(),
        'title': faker.sentence(),
        'description': faker.text(),
        'url': faker.url(),
        'badges': [],
        'created_at': faker.past_datetime().isoformat(),
        'datasets': faker.random_int(),
        'views': faker.random_int(),
        'followers': faker.random_int(),
        'featured': faker.random_int(min=0, max=1),
        'organization': {
            'id': faker.md5(),
            'name': faker.company(),
            'public_service': faker.random_int(min=0, max=1),
            'followers': faker.random_int(),
            'badges': [faker.word()]
        },
        'tags': [],
        'owner': None,
        'extras': {},
        'type': faker.word(),
        'topic': faker.word()
    }

    query = {
        'document': reuse,
        'index': index_name
    }

    index_resp = client.post(url_for('api.reuse_index'), json=query)
    assert index_resp.status_code == 200

    time.sleep(2)

    resp = search_client.es.get(index=f"{app.config['UDATA_INSTANCE_NAME']}-{index_name}", id=reuse['id'])
    assert resp['_source']['title'] == reuse['title']


def test_api_dataservice_index_unindex(app, client, faker):
    dataservice = {
        "id": faker.md5(),
        "title": faker.sentence(),
        "description": faker.text(),
        "created_at": faker.past_datetime().isoformat(),
        "followers": faker.random_int(),
        "views": faker.random_int(),
        'organization': {
            'id': faker.md5(),
            'name': faker.company(),
            'public_service': faker.random_int(min=0, max=1),
            'followers': faker.random_int()
        },
        "owner": None,
        "tags": [],
        "is_restricted": False,
        "extras":
        {
            "availability_url": "",
            "is_franceconnect": False,
            "public_cible":
            []
        }
    }

    query = {
        'document': dataservice,
        'index': None
    }

    index_resp = client.post(url_for('api.dataservice_index'), json={'document': dataservice, 'index': 'random-non-existing-index'})
    assert index_resp.status_code == 404

    index_resp = client.post(url_for('api.dataservice_index'), json=query)
    assert index_resp.status_code == 200

    time.sleep(2)

    dataservice_resp = client.get(url_for('api.dataservice_get_specific', dataservice_id=dataservice['id']))
    assert dataservice_resp.status_code == 200
    assert dataservice_resp.json['title'] == dataservice['title']

    dataservice_search_resp = client.get(url_for('api.dataservice_search'))
    assert len(dataservice_search_resp.json['data']) == 1
    assert dataservice_search_resp.json['data'][0]['title'] == dataservice['title']
    assert dataservice_search_resp.json['next_page'] is None
    assert dataservice_search_resp.json['page'] == 1
    assert dataservice_search_resp.json['previous_page'] is None
    assert dataservice_search_resp.json['page_size'] == 20
    assert dataservice_search_resp.json['total_pages'] == 1
    assert dataservice_search_resp.json['total'] == 1

    deletion_resp = client.delete(url_for('api.dataservice_unindex', dataservice_id=dataservice['id']))
    assert deletion_resp.status_code == 200

    time.sleep(2)

    dataservice_get_after_delete_resp = client.get(url_for('api.dataservice_get_specific', dataservice_id=dataservice['id']))
    assert dataservice_get_after_delete_resp.status_code == 404

    dataservice_search_after_delete_resp = client.get(url_for('api.dataservice_search'))
    assert len(dataservice_search_after_delete_resp.json['data']) == 0
    assert dataservice_search_after_delete_resp.json['next_page'] is None
    assert dataservice_search_after_delete_resp.json['page'] == 1
    assert dataservice_search_after_delete_resp.json['previous_page'] is None
    assert dataservice_search_after_delete_resp.json['page_size'] == 20
    assert dataservice_search_after_delete_resp.json['total_pages'] == 1
    assert dataservice_search_after_delete_resp.json['total'] == 0


def test_api_dataservice_index_on_another_index(app, client, search_client, faker):
    now = datetime.datetime.utcnow().strftime('%Y-%m-%d-%H-%M')
    index_name = f"test-dataservice-{now}"
    if not search_client.es.indices.exists(index=f"{app.config['UDATA_INSTANCE_NAME']}-{index_name}"):
        search_client.es.indices.create(index=f"{app.config['UDATA_INSTANCE_NAME']}-{index_name}")

    dataservice = {
        "id": faker.md5(),
        "title": faker.sentence(),
        "description": faker.text(),
        "created_at": faker.past_datetime().isoformat(),
        "followers": faker.random_int(),
        "views": faker.random_int(),
        'organization': {
            'id': faker.md5(),
            'name': faker.company(),
            'public_service': faker.random_int(min=0, max=1),
            'followers': faker.random_int()
        },
        "owner": None,
        "tags": [],
        "is_restricted": False,
        "extras":
        {
            "availability_url": "",
            "is_franceconnect": False,
            "public_cible":
            []
        }
    }

    query = {
        'document': dataservice,
        'index': index_name
    }

    index_resp = client.post(url_for('api.dataservice_index'), json=query)
    assert index_resp.status_code == 200

    time.sleep(2)

    resp = search_client.es.get(index=f"{app.config['UDATA_INSTANCE_NAME']}-{index_name}", id=dataservice['id'])
    assert resp['_source']['title'] == dataservice['title']


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


def test_api_create_index(app, client, search_client, faker):
    now = datetime.datetime.utcnow().strftime('%Y-%m-%d-%H-%M')
    index_name = f"test-dataset-{now}"

    payload = {
        'index': index_name
    }
    set_alias_resp = client.post(url_for('api.create_index'), json=payload)
    assert set_alias_resp.status_code == 200

    assert search_client.es.indices.exists(index=f"{app.config['UDATA_INSTANCE_NAME']}-{index_name}")


def test_api_set_index_alias(app, client, search_client, faker):
    now = datetime.datetime.utcnow().strftime('%Y-%m-%d-%H-%M')
    index_name = f"{app.config['UDATA_INSTANCE_NAME']}-dataset-{now}"

    if not search_client.es.indices.exists(index=index_name):
        search_client.es.indices.create(index=index_name)

    payload = {
        'index_suffix_name': now,
        'indices': ['dataset']
    }
    set_alias_resp = client.post(url_for('api.set_index_alias'), json=payload)
    assert set_alias_resp.status_code == 200

    index_alias = f"{app.config['UDATA_INSTANCE_NAME']}-dataset"

    assert search_client.es.indices.exists_alias(name=index_alias)
    alias_keys = list(search_client.es.indices.get_alias(name=index_alias))

    assert alias_keys[0] == index_name


    now = datetime.datetime.utcnow().strftime('%Y-%m-%d-%H-%M')

    for index in ['dataset', 'reuse', 'organization']:
        index_name = f"{app.config['UDATA_INSTANCE_NAME']}-{index}-{now}"
        if not search_client.es.indices.exists(index=index_name):
            search_client.es.indices.create(index=index_name)

    payload = {
        'index_suffix_name': now,
        'indices': []
    }
    set_alias_resp = client.post(url_for('api.set_index_alias'), json=payload)
    assert set_alias_resp.status_code == 200

    for index in ['dataset', 'reuse', 'organization']:

        index_alias = f"{app.config['UDATA_INSTANCE_NAME']}-{index}"
        index_name = f"{app.config['UDATA_INSTANCE_NAME']}-{index}-{now}"

        search_client.es.indices.exists_alias(name=index_alias)
        alias_keys = list(search_client.es.indices.get_alias(name=index_alias))

        assert alias_keys[0] == index_name


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
        search_client.index_dataset(DatasetFactory(tags=['test-tag', 'test-tag-2'] if i % 2 else ['not-test-tag']))
        search_client.index_reuse(ReuseFactory(tags=['test-tag'] if i % 2 else ['not-test-tag']))

    # Without this, ElasticSearch does not seem to have the time to index.
    time.sleep(2)

    # Filter is singular, we test the feature that maps it to the plural field of the entity.
    resp = client.get(url_for('api.dataset_search'))
    assert resp.json['total'] == 4

    resp = client.get(url_for('api.dataset_search', tag=['test-tag', 'test-tag-2']))
    assert resp.json['total'] == 2

    resp = client.get(url_for('api.dataset_search', tag=['not-test-tag']))
    assert resp.json['total'] == 2

    resp = client.get(url_for('api.reuse_search'))
    assert resp.json['total'] == 4

    resp = client.get(url_for('api.reuse_search', tag='test-tag'))
    assert resp.json['total'] == 2


def test_api_search_with_topic_filter(app, client, search_client, faker):
    for i in range(4):
        search_client.index_dataset(DatasetFactory(topics=['test_topic'] if i % 2 else ['not_test_topic']))

    # Without this, ElasticSearch does not seem to have the time to index.
    time.sleep(2)

    # Filter is singular, we test the feature that maps it to the plural field of the entity.
    resp = client.get(url_for('api.dataset_search'))
    assert resp.json['total'] == 4

    resp = client.get(url_for('api.dataset_search', topic='test_topic'))
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
