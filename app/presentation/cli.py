import click
from dependency_injector.wiring import inject, Provide
from flask import Flask
from flask.cli import with_appcontext
from app.container import Container
from app.domain.interfaces import SearchClient


@inject
def init_es(search_client: SearchClient = Provide[Container.search_client]) -> None:
    click.echo("Cleaning and creating indices.")
    search_client.clean_indices()
    click.echo("Done.")


@click.command("init-es")
@with_appcontext
def init_es_command() -> None:
    init_es()


def init_app(app: Flask) -> None:
    app.cli.add_command(init_es)
