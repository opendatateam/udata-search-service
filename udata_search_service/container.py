import os
from elasticsearch import Elasticsearch
from dependency_injector import containers, providers
from udata_search_service.infrastructure.services import DatasetService, OrganizationService, ReuseService
from udata_search_service.infrastructure.search_clients import ElasticClient


ELASTIC_HOST = os.environ.get('ELASTIC_HOST', 'localhost')
ELASTIC_PORT = os.environ.get('ELASTIC_PORT', '9200')


class Container(containers.DeclarativeContainer):
    config = providers.Configuration()

    elastic_client = providers.Singleton(
        Elasticsearch,
        [{'host': ELASTIC_HOST, 'port': ELASTIC_PORT}]
    )

    search_client = providers.Singleton(
        ElasticClient,
        url=config.elasticsearch_url
    )

    organization_service = providers.Factory(
        OrganizationService,
        search_client=search_client
    )

    dataset_service = providers.Factory(
        DatasetService,
        search_client=search_client
    )

    reuse_service = providers.Factory(
        ReuseService,
        search_client=search_client
    )
