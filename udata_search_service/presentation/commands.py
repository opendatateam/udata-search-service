import click
from flask.cli import with_appcontext
from dependency_injector.wiring import inject, Provide
from udata_search_service.container import Container
from udata_search_service.domain.interfaces import SearchClient
from udata_search_service.infrastructure.kafka_consumer import consume_kafka as consume_kafka_func


@inject
def init_es_func(search_client: SearchClient = Provide[Container.search_client]) -> None:
    click.echo("Cleaning and creating indices.")
    search_client.clean_indices()
    click.echo("Done.")


@click.command('init-es')
@with_appcontext
def init_es() -> None:
    init_es_func()


@click.command('consume-kafka')
@with_appcontext
def consume_kafka() -> None:
    consume_kafka_func()


def init_app(cli):
    cli.add_command(consume_kafka)
    cli.add_command(init_es)
