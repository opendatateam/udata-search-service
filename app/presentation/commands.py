import click
from dependency_injector.wiring import inject, Provide
from flask import Flask
from flask.cli import with_appcontext
from app.container import Container
from app.domain.interfaces import SearchClient
from app.infrastructure.kafka_consumer import consume_kafka
from app.infrastructure.migrate import set_alias


@inject
def init_es(search_client: SearchClient = Provide[Container.search_client]) -> None:
    click.echo("Setting up indices and aliases.")
    search_client.init_indices()
    click.echo("Done.")


@inject
def clean_es(search_client: SearchClient = Provide[Container.search_client]) -> None:
    click.echo("Cleaning and creating indices and aliases.")
    search_client.clean_indices()
    click.echo("Done.")


@click.command("init-es")
@with_appcontext
def init_es_command() -> None:
    init_es()


@click.command("clean-es")
@with_appcontext
def clean_es_command() -> None:
    clean_es()


@click.command("consume-kafka")
@with_appcontext
def consume_kafka_command() -> None:
    consume_kafka()


@click.command("set-alias")
@click.argument("index_suffix_to_use")
@with_appcontext
def set_alias_command(index_suffix_to_use) -> None:
    set_alias(index_suffix_to_use)


def init_app(app: Flask) -> None:
    app.cli.add_command(consume_kafka_command)
    app.cli.add_command(init_es_command)
    app.cli.add_command(clean_es_command)
    app.cli.add_command(set_alias_command)
