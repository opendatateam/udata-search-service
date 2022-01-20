from flask import Flask
from app.config import Config
from app.container import Container
from app.infrastructure import kafka_consumer, search_clients
from app.presentation import api, cli


def create_app(config: object = Config) -> Flask:
    container = Container()
    container.wire(modules=[api, cli])

    app: Flask = Flask(__name__)
    app.container = container
    app.config.from_object(config)

    container.config.elasticsearch_url.from_value(app.config['ELASTICSEARCH_URL'])
    container.config.search_synonyms.from_value(app.config['SEARCH_SYNONYMS'])

    # register the database command
    cli.init_app(app)
    kafka_consumer.init_app(app)

    app.register_blueprint(api.bp)

    return app
