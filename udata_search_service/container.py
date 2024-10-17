from dependency_injector import containers, providers
from udata_search_service.infrastructure.services import DatasetService, OrganizationService, ReuseService, DataserviceService
from udata_search_service.infrastructure.search_clients import ElasticClient


class Container(containers.DeclarativeContainer):
    config = providers.Configuration()

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

    dataservice_service = providers.Factory(
        DataserviceService,
        search_client=search_client
    )
