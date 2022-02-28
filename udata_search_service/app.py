import click
from flask import Flask
from flask.cli import FlaskGroup
from udata_search_service.config import Config
from udata_search_service.container import Container
from udata_search_service.presentation import api, commands


def create_app(config: object = Config) -> Flask:
    container = Container()
    container.wire(modules=[api, commands])

    app: Flask = Flask(__name__)
    app.container = container
    app.config.from_object(config)

    container.config.elasticsearch_url.from_value(app.config['ELASTICSEARCH_URL'])
    container.config.search_synonyms.from_value(app.config['SEARCH_SYNONYMS'])

    app.register_blueprint(api.bp)

    return app


@click.group(cls=FlaskGroup, create_app=create_app)
def cli():
    '''udata-search-service management client'''


commands.init_app(cli)
