import click
from flask.cli import with_appcontext
from dependency_injector.wiring import inject, Provide
from udata_search_service.container import Container
from udata_search_service.domain.interfaces import SearchClient


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


def init_app(cli):
    cli.add_command(init_es)
    cli.add_command(clean_es_command)
