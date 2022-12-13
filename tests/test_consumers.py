import copy
import datetime

from udata_search_service.infrastructure.consumers import ReuseConsumer, OrganizationConsumer, DatasetConsumer
from udata_search_service.infrastructure.utils import get_concat_title_org, log2p, mdstrip


def test_parse_dataset_obj():
    obj = {
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
        'schema_': ['etalab/schema-irve']
    }
    document = DatasetConsumer.load_from_dict(copy.deepcopy(obj)).to_dict()

    # Make sure that these fields are loaded as is
    for key in ['id', 'title', 'url', 'frequency', 'resources_count',
                'acronym', 'badges', 'tags', 'license', 'owner']:
        assert document[key] == obj[key]

    # Make sure that markdown fields are stripped
    assert document["description"] == mdstrip(obj["description"])

    # Make sure schema keyword is retrieved correctly
    assert document["schema"] == obj["schema_"]

    # Make sure that these fields are log2p-normalized
    for key in ['views', 'followers', 'reuses']:
        assert document[key] == log2p(obj[key])

    # Make sure that all other particular fields are treated accordingly
    assert document['concat_title_org'] == get_concat_title_org(document['title'], document['acronym'], document['organization_name'])
    assert document['created_at'].date() == datetime.date(2019, 1, 25)
    assert document['temporal_coverage_start'].date() == datetime.date(2016, 7, 1)
    assert document['temporal_coverage_end'].date() == datetime.date(2021, 6, 30)
    assert document['granularity'] == 'fr:commune'
    assert document['geozones'] == ['fr:arrondissement:353', 'country-group:world', 'country:fr', 'country-group:ue']
    assert document['organization'] == obj['organization']['id']
    assert document['organization_name'] == obj['organization']['name']
    assert document['orga_followers'] == log2p(401)


def test_parse_reuse_obj():
    obj = {
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
    document = ReuseConsumer.load_from_dict(copy.deepcopy(obj)).to_dict()

    # Make sure that these fields are loaded as is
    for key in ['id', 'title', 'url', 'datasets', 'featured',
                'badges', 'tags', 'owner']:
        assert document[key] == obj[key]

    # Make sure that markdown fields are stripped
    assert document["description"] == mdstrip(obj["description"])

    # Make sure that these fields are log2p-normalized
    for key in ['views', 'followers']:
        assert document[key] == log2p(obj[key])

    # Make sure that all other particular fields are treated accordingly
    assert document['created_at'].date() == datetime.date(2019, 4, 26)
    assert document['organization'] == obj['organization']['id']
    assert document['organization_name'] == obj['organization']['name']
    assert document['orga_followers'] == log2p(obj['organization']['followers'])


def test_parse_organization_obj():
    obj = {
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
    document = OrganizationConsumer.load_from_dict(copy.deepcopy(obj)).to_dict()

    # Make sure that these fields are loaded as is
    for key in ['id', 'name', 'acronym', 'url', 'badges', 'orga_sp', 'datasets', 'reuses']:
        assert document[key] == obj[key]

    # Make sure that markdown fields are stripped
    assert document["description"] == mdstrip(obj["description"])

    # Make sure that these fields are log2p-normalized
    assert document["followers"] == log2p(obj["followers"])
    assert document["views"] == log2p(obj["views"])

    # Make sure that all other particular fields are treated accordingly
    assert document['created_at'].date() == datetime.date(2014, 4, 17)
