import secrets
from typing import List

from pydantic import AnyHttpUrl, BaseSettings


class Settings(BaseSettings):
    API_V1_STR: str = "/api/v1"
    SECRET_KEY: str = secrets.token_urlsafe(32)
    # 60 minutes * 24 hours * 8 days = 8 days
    ACCESS_TOKEN_EXPIRE_MINUTES: int = 60 * 24 * 8
    SERVER_NAME: str = None
    SERVER_HOST: AnyHttpUrl = 'http://127.0.0.1:7000'

    PROJECT_NAME: str = 'Udata Search Service'

    ELASTICSEARCH_ADDR: str = 'localhost'
    ELASTICSEARCH_PORT: str = '9200'
    ELASTICSEARCH_URL: AnyHttpUrl = f'http://{ELASTICSEARCH_ADDR}:{ELASTICSEARCH_PORT}'

    DATASET_CATALOG_URL: AnyHttpUrl = 'https://www.data.gouv.fr/fr/datasets/r/f868cca6-8da1-4369-a78d-47463f19a9a3'
    ORG_CATALOG_URL: AnyHttpUrl = 'https://www.data.gouv.fr/fr/datasets/r/b7bbfedc-2448-4135-a6c7-104548d396e7'

    SEARCH_SYNONYMS: List = [
        "AMD, administrateur ministériel des données, AMDAC",
        "lolf, loi de finance",
        "waldec, RNA, répertoire national des associations",
        "ovq, baromètre des résultats",
        "contour, découpage"
    ]

    class Config:
        case_sensitive = True


settings = Settings()
