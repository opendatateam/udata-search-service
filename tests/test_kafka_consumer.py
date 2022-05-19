import datetime
import json

from udata_search_service.infrastructure.kafka_consumer import parse_message
from udata_search_service.infrastructure.utils import get_concat_title_org, log2p, mdstrip


def test_parse_dataset_message():
    message = {
        'service': 'udata',
        'meta': {
            'message_type': 'dataset.index',
            'index': 'dataset'
        },
        'data': {
            'id': '5c4ae55a634f4117716d5656',
            'title': 'Demandes de valeurs foncières',
            'description': '### Propos liminaires...',
            'acronym': 'DVF',
            'url': '/fr/datasets/demandes-de-valeurs-foncieres/',
            'tags': ['foncier', 'foncier-sol-mutation-fonciere', 'fonciere', 'valeur-fonciere'],
            'license': 'notspecified',
            'badges': [],
            'frequency': 'semiannual',
            'created_at': '2019-01-25T11:30:50',
            'views': 7806,
            'followers': 72,
            'reuses': 45,
            'featured': 0,
            'resources_count': 10,
            'organization': {
                'id': '534fff8ea3a7292c64a77f02',
                'name': 'Ministère de l\'économie, des finances et de la relance',
                'public_service': 1,
                'followers': 401},
            'owner': None,
            'format': ['pdf', 'pdf', 'pdf', 'pdf', 'txt', 'txt', 'txt', 'txt', 'txt', 'txt'],
            'temporal_coverage_start': '2016-07-01T00:00:00',
            'temporal_coverage_end': '2021-06-30T00:00:00',
            'geozones': [{'id': 'fr:arrondissement:353', 'name': 'Rennes', 'keys': ['353']},
                         {'id': 'country-group:world'},
                         {'id': 'country:fr'},
                         {'id': 'country-group:ue'}],
            'granularity': 'fr:commune',
            'schema': ['etalab/schema-irve']
        }
    }
    val_utf8 = json.dumps(message)
    message_type, index_name, data = parse_message(val_utf8)

    assert message_type == 'index'
    assert index_name == 'dataset'

    # Make sure that these fields are loaded as is
    for key in ['id', 'title', 'url', 'frequency', 'resources_count',
                'acronym', 'badges', 'tags', 'license', 'owner', 'schema']:
        assert data[key] == message['data'][key]

    # Make sure that markdown fields are stripped
    assert data["description"] == mdstrip(message['data']["description"])

    # Make sure that these fields are log2p-normalized
    for key in ['views', 'followers', 'reuses']:
        assert data[key] == log2p(message['data'][key])

    # Make sure that boolean fields are either 1 or 4
    assert data['featured'] == 1
    assert data['orga_sp'] == 4

    # Make sure that all other particular fields are treated accordingly
    assert data['concat_title_org'] == get_concat_title_org(data['title'], data['acronym'], data['organization_name'])
    assert data['created_at'].date() == datetime.date(2019, 1, 25)
    assert data['temporal_coverage_start'].date() == datetime.date(2016, 7, 1)
    assert data['temporal_coverage_end'].date() == datetime.date(2021, 6, 30)
    assert data['granularity'] == 'fr:commune'
    assert data['geozones'] == ['fr:arrondissement:353', 'country-group:world', 'country:fr', 'country-group:ue']
    assert data['organization'] == message['data']['organization']['id']
    assert data['organization_name'] == message['data']['organization']['name']
    assert data['orga_followers'] == log2p(401)


def test_parse_reuse_message():
    message = {
        'service': 'udata',
        'meta': {
            'message_type': 'reuse.index',
            'index': 'reuse'
        },        'data': {
            "id": "5cc2dfbe8b4c414c91ffc46d",
            "title": "Explorateur de données de valeur foncière (DVF)",
            "description": "Cartographie des mutations à titre onéreux (parcelles en bleu).",
            "url": "https://app.dvf.etalab.gouv.fr/",
            "created_at": "2019-04-26T12:38:54",
            "views": 4326,
            "followers": 11,
            "datasets": 2,
            "featured": 1,
            "organization": {
                "id": "534fff75a3a7292c64a77de4",
                "name": "Etalab",
                "public_service": 1,
                "followers": 357},
            "owner": None,
            "type": "application",
            "topic": "housing_and_development",
            "tags": ["application-cartographique", "cadastre", "dgfip", "dvf", "etalab", "foncier", "mutations"],
            "badges": []
        }
    }
    val_utf8 = json.dumps(message)
    message_type, index_name, data = parse_message(val_utf8)

    assert message_type == 'index'
    assert index_name == 'reuse'

    # Make sure that these fields are loaded as is
    for key in ['id', 'title', 'url', 'datasets', 'featured',
                'badges', 'tags', 'owner']:
        assert data[key] == message['data'][key]

    # Make sure that markdown fields are stripped
    assert data["description"] == mdstrip(message['data']["description"])

    # Make sure that these fields are log2p-normalized
    for key in ['views', 'followers']:
        assert data[key] == log2p(message['data'][key])

    # Make sure that all other particular fields are treated accordingly
    assert data['created_at'].date() == datetime.date(2019, 4, 26)
    assert data['organization'] == message['data']['organization']['id']
    assert data['organization_name'] == message['data']['organization']['name']
    assert data['orga_followers'] == log2p(message['data']['organization']['followers'])


def test_parse_organization_message():
    message = {
        'service': 'udata',
        'meta': {
            'message_type': 'organization.index',
            'index': 'organization'
        },        'data': {
            "id": "534fff75a3a7292c64a77de4",
            "name": "Etalab",
            "acronym": None,
            "description": "Etalab est un département de la direction interministérielle du numérique (DINUM)",
            "url": "https://www.etalab.gouv.fr",
            "badges": ["public-service", "certified"],
            "created_at": "2014-04-17T18:21:09",
            "orga_sp": 1,
            "followers": 357,
            "datasets": 56,
            "views": 42,
            "reuses": 0
        }
    }
    val_utf8 = json.dumps(message)
    message_type, index_name, data = parse_message(val_utf8)

    assert message_type == 'index'
    assert index_name == 'organization'

    # Make sure that these fields are loaded as is
    for key in ['id', 'name', 'acronym', 'url', 'badges', 'orga_sp', 'datasets', 'reuses']:
        assert data[key] == message['data'][key]

    # Make sure that markdown fields are stripped
    assert data["description"] == mdstrip(message['data']["description"])

    # Make sure that these fields are log2p-normalized
    assert data["followers"] == log2p(message['data']["followers"])
    assert data["views"] == log2p(message['data']["views"])

    # Make sure that all other particular fields are treated accordingly
    assert data['created_at'].date() == datetime.date(2014, 4, 17)
