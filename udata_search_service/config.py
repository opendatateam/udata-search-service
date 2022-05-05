import os


class Config:
    SECRET_KEY = os.environ.get('SECRET_KEY') or 'you-will-never-guess'

    ELASTICSEARCH_URL = os.environ.get('ELASTICSEARCH_URL') or 'http://localhost:9200'

    UDATA_INSTANCE_NAME = os.environ.get('UDATA_INSTANCE_NAME') or 'udata'

    SEARCH_SYNONYMS = [
        "AMD, administrateur ministériel des données, AMDAC",
        "lolf, loi de finance",
        "waldec, RNA, répertoire national des associations",
        "ovq, baromètre des résultats",
        "contour, découpage",
        "rp, recensement de la population"
    ]


class Testing(Config):
    TESTING = True
    ELASTICSEARCH_URL = os.environ.get('ELASTICSEARCH_URL_TEST') or 'http://localhost:9201'
