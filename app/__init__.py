from flask import Flask
from app.config import Config
from app.container import Container
from app.presentation import api, commands


def create_app(config: object = Config) -> Flask:
    container = Container()
    container.wire(modules=[api, commands])

    app: Flask = Flask(__name__)
    app.container = container
    app.config.from_object(config)

    container.config.elasticsearch_url.from_value(app.config['ELASTICSEARCH_URL'])
    container.config.search_synonyms.from_value(app.config['SEARCH_SYNONYMS'])

    # register the database command
    commands.init_app(app)

    app.register_blueprint(api.bp)

    return app
