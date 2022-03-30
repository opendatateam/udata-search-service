import os


class Config:
    SECRET_KEY = os.environ.get('SECRET_KEY') or 'you-will-never-guess'

    ELASTICSEARCH_URL = os.environ.get('ELASTICSEARCH_URL') or 'http://localhost:9200'

    DATASET_CATALOG_URL = 'https://www.data.gouv.fr/fr/datasets/r/f868cca6-8da1-4369-a78d-47463f19a9a3'
    ORG_CATALOG_URL = 'https://www.data.gouv.fr/fr/datasets/r/b7bbfedc-2448-4135-a6c7-104548d396e7'
    REUSE_CATALOG_URL = 'https://www.data.gouv.fr/fr/datasets/r/970aafa0-3778-4d8b-b9d1-de937525e379'

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
