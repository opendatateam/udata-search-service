import json
import pandas as pd
from tempfile import NamedTemporaryFile

import click
from app.config import settings
from app.domain.entities import Dataset, Organization, Reuse
from app.infrastructure.search_clients import ElasticClient
from app.infrastructure.services import OrganizationService, DatasetService, ReuseService
from app.infrastructure.utils import download_catalog


def process_org_catalog(organization_service: OrganizationService):
    click.echo("Processing organizations data.")
    with NamedTemporaryFile(delete=False) as org_fd:
        download_catalog(settings.ORG_CATALOG_URL, org_fd)
    with open(org_fd.name) as org_csvfile:
        # Dataframe catalogue orga
        dfo = pd.read_csv(org_csvfile, dtype="str", sep=";")

        # Récupèration de l'information "service public" depuis la colonne badge.
        # Attribution de la valeur 4 si c'est un SP, 1 si ça ne l'est pas
        dfo['orga_sp'] = dfo['badges'].apply(lambda x: 4 if 'public-service' in x else 1)

        # Sauvegarde en mémoire du dataframe avec uniquement les infos pertinentes
        dfo = dfo[['id', 'name', 'description', 'url', 'orga_sp', 'metric.followers', 'metric.datasets', 'created_at']]

        # Renommage de l'id de l'organisation et de la métrique followers
        dfo = dfo.rename(columns={
            'metric.followers': 'orga_followers',
            'metric.datasets': 'orga_datasets'
        })

        dfo['orga_followers'] = dfo['orga_followers'].astype(float)

        fobins = [-1, 0, 10, 50, 100, dfo['orga_followers'].max()]
        dfo['orga_followers'] = pd.cut(dfo['orga_followers'], fobins, labels=list(range(1, 6)))

        dfo_as_json = dfo.to_json(orient='records', lines=True)
        with click.progressbar(dfo_as_json.split('\n')) as bar:
            for json_document in bar:
                if json_document != '':
                    # Convertion de la string json en dictionnaire
                    jdict = json.loads(json_document)
                    if jdict['orga_datasets'] != '0':
                        organization_service.feed(Organization(**jdict))


def process_dataset_catalog(dataset_service: DatasetService):
    click.echo("Processing datasets data.")
    with NamedTemporaryFile(delete=False) as dataset_fd:
        download_catalog(settings.DATASET_CATALOG_URL, dataset_fd)
    with NamedTemporaryFile(delete=False) as org_fd:
        download_catalog(settings.ORG_CATALOG_URL, org_fd)

    with open(dataset_fd.name) as dataset_csvfile, open(org_fd.name) as org_csvfile:
        # Dataframe catalogue dataset
        dfd = pd.read_csv(dataset_csvfile, dtype=str, sep=";")
        # Dataframe catalogue orga
        dfo = pd.read_csv(org_csvfile, dtype="str", sep=";")

        # Récupèration de l'information "service public" depuis la colonne badge.
        # Attribution de la valeur 4 si c'est un SP, 1 si ça ne l'est pas
        dfo['orga_sp'] = dfo['badges'].apply(lambda x: 4 if 'public-service' in x else 1)

        # Sauvegarde en mémoire du dataframe avec uniquement les infos pertinentes
        dfo = dfo[['logo', 'id', 'orga_sp', 'metric.followers']]

        # Renommage de l'id de l'organisation et de la métrique followers
        dfo = dfo.rename(columns={
            'id': 'organization_id',
            'metric.followers': 'orga_followers'
        })

        # Merge des deux dataframes (dataset et orga) pour n'en garder qu'un seul unique
        # La colonne de jointure est l'id de l'organisation. On fait un merge de type left join
        df = pd.merge(dfd, dfo, on='organization_id', how='left')

        # Modification de certaines colonnes pour optimiser par la suite la recherche ES
        # Les trois colonnes metric.views metric.followers et es_orga_followers sont converties en float
        # (elles étaient préalablement des string)
        df['metric.views'] = df['metric.views'].astype(float)
        df['metric.followers'] = df['metric.followers'].astype(float)
        df['orga_followers'] = df['orga_followers'].astype(float)

        # Afin qu'une métrique ne soit pas plus d'influente que l'autre, on normalise chacune des valeurs de ces colonnes
        # dans 5 catégorie (de 1 à 5)
        # Les "bins" utilisés pour chacune d'entre elles sont préparées "à la main". (difficile en effet d'avoir des bins
        # automatiques en sachant que l'immense majorité des datasets ont une valeur de 0 pour ces 3 métriques)
        # Comment lire :
        # Datasets de 0 vues = valeur 1
        # Datasets entre 1 et 49 vues = valeur 2
        # Datasets entre 50 et 499 vues = valeur 3
        # Datasets entre 500 et 4999 vues = Valeur 4
        # Datasets entre 5000 et 'nombre de vues max d'un dataset' = Valeur 5
        mvbins = [-1, 0, 50, 500, 5000, df['metric.views'].max()]
        df['dataset_views'] = pd.cut(df['metric.views'], mvbins, labels=list(range(1, 6)))
        mfbins = [-1, 0, 2, 10, 40, df['metric.followers'].max()]
        df['dataset_followers'] = pd.cut(df['metric.followers'], mfbins, labels=list(range(1, 6)))
        fobins = [-1, 0, 10, 50, 100, df['orga_followers'].max()]
        df['orga_followers'] = pd.cut(df['orga_followers'], fobins, labels=list(range(1, 6)))

        # Création d'un champs "es_concat_title_org" concatenant le nom du titre et de l'organisation (de nombreuses recherches concatènent ces deux types de données)
        df['concat_title_org'] = df['title'] + ' ' + df['acronym'] + ' ' + df['organization']

        # Création d'un champ "es_dataset_featured" se basant sur la colonne features. L'objectif étant de donner un poids plus grand aux datasets featured
        # Poids de 5 quand le dataset est featured, 1 sinon
        df['dataset_featured'] = df['featured'].apply(lambda x: 5 if x == 'True' else 1)

        # Renommage de l'id de l'organisation et de la métrique followers
        df = df.rename(columns={
            'temporal_coverage.start': 'temporal_coverage_start',
            'temporal_coverage.end': 'temporal_coverage_end',
            'spatial.granularity': 'spatial_granularity',
            'spatial.zones': 'spatial_zones',
            'metric.reuses': 'dataset_reuses'
        })

        # Suppresion des jeux de données archivés ou privés
        df = df[df.archived == 'False']

        # Sauvegarde en mémoire du dataframe avec uniquement les infos pertinentes
        df = df[[
            'id',
            'title',
            'url',
            'created_at',
            'acronym',
            'organization',
            'organization_id',
            'description',
            'orga_sp',
            'orga_followers',
            'dataset_views',
            'dataset_followers',
            'resources_count',
            'concat_title_org',
            'dataset_featured',
            'temporal_coverage_start',
            'temporal_coverage_end',
            'spatial_granularity',
            'spatial_zones',
            'dataset_reuses'
        ]]
        # Convertion du dataframe en string json séparée par des \n
        df_as_json = df.to_json(orient='records', lines=True)

        with click.progressbar(df_as_json.split('\n')) as bar:
            for json_document in bar:
                if json_document != '':
                    # Convertion de la string json en dictionnaire
                    jdict = json.loads(json_document)
                    if jdict['resources_count'] != '0':
                        dataset_service.feed(Dataset(**jdict))


def process_reuse_catalog(reuse_service: ReuseService):
    click.echo("Processing reuses data.")
    with NamedTemporaryFile(delete=False) as reuse_fd:
        download_catalog(settings.REUSE_CATALOG_URL, reuse_fd)
    with NamedTemporaryFile(delete=False) as org_fd:
        download_catalog(settings.ORG_CATALOG_URL, org_fd)
    with open(reuse_fd.name) as reuse_csvfile, open(org_fd.name) as org_csvfile:
        # Dataframe catalogue reuse
        dfr = pd.read_csv(reuse_csvfile, dtype="str", sep=";")
        # Dataframe catalogue orga
        dfo = pd.read_csv(org_csvfile, dtype="str", sep=";")

        # Sauvegarde en mémoire du dataframe avec uniquement les infos pertinentes
        dfo = dfo[['id', 'metric.followers']]

        # Renommage de l'id de l'organisation et de la métrique followers
        dfo = dfo.rename(columns={
            'id': 'organization_id',
            'metric.followers': 'orga_followers'
        })

        # Merge des deux dataframes (dataset et orga) pour n'en garder qu'un seul unique
        # La colonne de jointure est l'id de l'organisation. On fait un merge de type left join
        df = pd.merge(dfr, dfo, on='organization_id', how='left')

        df['metric.views'] = df['metric.views'].astype(float)
        df['metric.followers'] = df['metric.followers'].astype(float)
        df['orga_followers'] = df['orga_followers'].astype(float)

        mvbins = [-1, 0, 50, 500, 5000, df['metric.views'].max()]
        df['reuse_views'] = pd.cut(df['metric.views'], mvbins, labels=list(range(1, 6)))
        mfbins = [-1, 0, 2, 10, 40, df['metric.followers'].max()]
        df['reuse_followers'] = pd.cut(df['metric.followers'], mfbins, labels=list(range(1, 6)))
        fobins = [-1, 0, 10, 50, 100, df['orga_followers'].max()]
        df['orga_followers'] = pd.cut(df['orga_followers'], fobins, labels=list(range(1, 6)))

        df['reuse_featured'] = df['featured'].apply(lambda x: 5 if x == 'True' else 1)

        df = df.rename(columns={'metric.datasets': 'reuse_datasets'})

        df = df[[
            'id',
            'title',
            'url',
            'created_at',
            'organization',
            'organization_id',
            'description',
            'orga_followers',
            'reuse_views',
            'reuse_followers',
            'reuse_datasets',
            'reuse_featured'
        ]]
        # Convertion du dataframe en string json séparée par des \n
        df_as_json = df.to_json(orient='records', lines=True)

        with click.progressbar(df_as_json.split('\n')) as bar:
            for json_document in bar:
                if json_document != '':
                    # Convertion de la string json en dictionnaire
                    jdict = json.loads(json_document)
                    if jdict['reuse_datasets'] != '0':
                        reuse_service.feed(Reuse(**jdict))


def seed_db() -> None:
    click.echo("Cleaning indices.")
    search_client = ElasticClient()
    organization_service = OrganizationService(search_client=search_client)
    dataset_service = DatasetService(search_client=search_client)
    reuse_service = ReuseService(search_client=search_client)
    search_client.clean_indices()
    click.echo("Done.")
    process_org_catalog(organization_service)
    process_dataset_catalog(dataset_service)
    process_reuse_catalog(reuse_service)
    click.echo("Done.")


if __name__ == "__main__":
    seed_db()
