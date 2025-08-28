import copy
import datetime

from udata_search_service.infrastructure.consumers import ReuseConsumer, OrganizationConsumer, DataserviceConsumer, DatasetConsumer
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
        'last_update': '2020-02-27T12:32:50',
        'views': 7806,
        'followers': 72,
        'reuses': 45,
        'featured': 0,
        'resources_count': 10,
        'resources': [
            {"id": "5ffa8553-0e8f-4622-add9-5c0b593ca1f8", "title": "Valeurs foncières 2024"},
            {"id": "bc213c7c-c4d4-4385-bf1f-719573d39e90", "title": "Valeurs foncières 2023"},
        ],
        'organization': {
            'id': '534fff8ea3a7292c64a77f02',
            'name': 'Ministère de l\'économie, des finances et de la relance',
            'public_service': 1,
            'followers': 401,
            'badges': ["public-service"]},
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
    assert document['last_update'].date() == datetime.date(2020, 2, 27)
    assert document['temporal_coverage_start'].date() == datetime.date(2016, 7, 1)
    assert document['temporal_coverage_end'].date() == datetime.date(2021, 6, 30)
    assert document['granularity'] == 'fr:commune'
    assert document['geozones'] == ['fr:arrondissement:353', 'country-group:world', 'country:fr', 'country-group:ue']
    assert document['organization'] == obj['organization']['id']
    assert document['organization_name'] == obj['organization']['name']
    assert document['organization_badges'] == obj['organization']['badges']
    assert document['orga_followers'] == log2p(401)
    assert document['resources_ids'] == [res["id"] for res in obj["resources"]]
    assert document['resources_titles'] == [res["title"] for res in obj["resources"]]


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
            "followers": 357,
            'badges': ["public-service"]},
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
    assert document['organization_badges'] == obj['organization']['badges']
    assert document['orga_followers'] == log2p(obj['organization']['followers'])


def test_parse_dataservice_obj():
    obj = {
        "id": "670786ec0e65e6a0c784bf24",
        "title": "API INES",
        "description": "### À quoi sert l'API INES ?\n\nL’API INES (Identifiant National dans l'Enseignement Supérieur) permet la vérification et l’immatriculation des étudiants dans l’enseignement supérieur.\nLa vérification s’effectue à partir des données d’état civil de l’étudiant et permet de récupérer son identifiant national étudiant unique s’il est déjà immatriculé.\nL’immatriculation permet d’obtenir un identifiant national unique pour l’étudiant, qui lui sera associé durant tout son parcours d’étudiant.\n\nINES permet une immatriculation unique de tout étudiant:\n- dans l’ensemble des établissements, indépendamment de ses mobilités\n- dans tous les autres systèmes d’information sur les étudiants\nL'identifiant national de l’étudiant est une donnée pivot pour mettre en relation l’ensemble des SI les concernant, qu’ils émanent des établissements, du Ministère de l'Enseignement Supérieur et de la Recherche et des administrations publics (StatutEtudiant, remontées SISE), ou des opérateurs (PARCOURSUP, CNOUS…).\n\n\n## Principales données disponibles\n\nL’API s’adresse à tous les établissements proposant une formation dans l’enseignement supérieur et aux systèmes d’information dans l'enseignement supérieur, comme les plateformes de candidature aux établissements d’enseignement supérieur ou le CNOUS.\n\nL’obtention d’un identifiant national étudiant (INE) d’INES se fait à partir des données d’état civil de l’étudiant.\n\nUne authentification (identifiants d’accès directement fourni par le MESR aux établissements) est nécessaire. Sinon, il n’y a pas de restriction particulière quant à l’utilisation de cette API.\n\n[Lien des documentations supplémentaires](https://ines.enseignementsup-recherche.gouv.fr)",
        "created_at": "2024-10-10T07:49:00",
        "views": 4326,
        "followers": 0,
        "organization":
        {
            "id": "534fff90a3a7292c64a77f45",
            "name": "Ministère de l'Enseignement supérieur et de la Recherche",
            "public_service": 1,
            "followers": 152
        },
        "owner": None,
        "tags":
        [
            "education",
            "enseignement",
            "identifiant",
            "immatriculation",
            "ines",
            "verification"
        ],
        "is_restricted": False,
        "extras":
        {
            "availability_url": "",
            "is_franceconnect": False,
            "public_cible":
            []
        }
    }
    document = DataserviceConsumer.load_from_dict(copy.deepcopy(obj)).to_dict()

    # Make sure that these fields are loaded as is
    for key in ['id', 'title', 'tags', 'owner', 'is_restricted']:
        assert document[key] == obj[key]

    # Make sure that markdown fields are stripped
    assert document["description"] == mdstrip(obj["description"])

    # Make sure that these fields are log2p-normalized
    for key in ['views', 'followers']:
        assert document[key] == log2p(obj[key])

    # Make sure that all other particular fields are treated accordingly
    assert document['created_at'].date() == datetime.date(2024, 10, 10)
    assert document['organization'] == obj['organization']['id']
    assert document['organization_name'] == obj['organization']['name']
    assert document['orga_followers'] == log2p(obj['organization']['followers'])
    assert document['description_length'] == log2p(len(mdstrip(obj['description'])))


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
