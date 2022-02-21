import click
from dependency_injector.wiring import inject, Provide
from flask.cli import shell_command, with_appcontext, FlaskGroup
from udata_search_service.app import create_app
from udata_search_service.container import Container
from udata_search_service.domain.interfaces import SearchClient
from udata_search_service.infrastructure.kafka_consumer import consume_kafka as consume_kafka_func


@inject
def init_es_func(search_client: SearchClient = Provide[Container.search_client]) -> None:
    click.echo("Cleaning and creating indices.")
    search_client.clean_indices()
    click.echo("Done.")


@click.group(cls=FlaskGroup, create_app=create_app)
def cli():
    '''udata-search-service management client'''


@cli.command()
def init_es() -> None:
    init_es_func()


@cli.command()
def consume_kafka() -> None:
    consume_kafka_func()
