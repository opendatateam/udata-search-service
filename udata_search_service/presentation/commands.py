import click
from flask.cli import with_appcontext
from dependency_injector.wiring import inject, Provide
from udata_search_service.container import Container
from udata_search_service.domain.interfaces import SearchClient
from udata_search_service.infrastructure.kafka_consumer import consume_kafka as consume_kafka_func
from udata_search_service.infrastructure.migrate import set_alias as set_alias_func


@inject
def init_es_func(search_client: SearchClient = Provide[Container.search_client]) -> None:
    click.echo("Setting up indices and aliases if they don't exist.")
    search_client.init_indices()
    click.echo("Done.")


@inject
def clean_es_func(search_client: SearchClient = Provide[Container.search_client]) -> None:
    click.echo("Removing previous indices and intializing new ones.")
    search_client.clean_indices()
    click.echo("Done.")


@click.command('init-es')
@with_appcontext
def init_es() -> None:
    init_es_func()


@click.command("clean-es")
@with_appcontext
def clean_es_command() -> None:
    clean_es_func()


@click.command('consume-kafka')
@with_appcontext
def consume_kafka() -> None:
    consume_kafka_func()


@click.command("set-alias")
@click.argument("index_suffix_to_use")
@click.argument('indices', nargs=-1, metavar='[<index> ...]')
@with_appcontext
def set_alias_command(index_suffix_to_use, indices=None) -> None:
    indices = [index.lower().rstrip('s') for index in (indices or [])]
    set_alias_func(index_suffix_to_use, indices)


def init_app(cli):
    cli.add_command(consume_kafka)
    cli.add_command(init_es)
    cli.add_command(clean_es_command)
    cli.add_command(set_alias_command)
