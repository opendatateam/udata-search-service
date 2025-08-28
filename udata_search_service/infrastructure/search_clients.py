import click
from datetime import datetime
from fnmatch import fnmatch
import logging
import os
from typing import Tuple, Optional, List

from elasticsearch import Elasticsearch
from elasticsearch.exceptions import NotFoundError
from elasticsearch_dsl import Date, Document, Float, Integer, Keyword, Text, tokenizer, token_filter, analyzer, query
from elasticsearch_dsl.connections import connections
from udata_search_service.domain.entities import Dataset, Organization, Reuse, Dataservice
from udata_search_service.config import Config
from udata_search_service.infrastructure.utils import IS_TTY

CONSUMER_LOGGING_LEVEL = int(os.environ.get("CONSUMER_LOGGING_LEVEL", logging.INFO))

logging.basicConfig(level=CONSUMER_LOGGING_LEVEL)


# Définition d'un analyzer français (repris ici : https://jolicode.com/blog/construire-un-bon-analyzer-francais-pour-elasticsearch)
# Ajout dans la filtre french_synonym, des synonymes que l'on souhaite implémenter (ex : AMD / Administrateur des Données)
# Création du mapping en indiquant les champs sur lesquels cet analyzer va s'appliquer (title, description, concat, organization)
# et en spécifiant les types de champs que l'on va utiliser pour calculer notre score de pertinence
french_elision = token_filter('french_elision', type='elision', articles_case=True, articles=["l", "m", "t", "qu", "n", "s", "j", "d", "c", "jusqu", "quoiqu", "lorsqu", "puisqu"])
french_stop = token_filter('french_stop', type='stop', stopwords='_french_')
french_stemmer = token_filter('french_stemmer', type='stemmer', language='light_french')
french_synonym = token_filter('french_synonym', type='synonym', ignore_case=True, expand=True, synonyms=Config.SEARCH_SYNONYMS)


dgv_analyzer = analyzer('french_dgv',
                        tokenizer=tokenizer('icu_tokenizer'),
                        filter=['icu_folding', french_elision, french_synonym, french_stemmer, french_stop]
                        )


class IndexDocument(Document):

    @classmethod
    def init_index(cls, es_client: Elasticsearch, suffix: str) -> None:
        alias = cls._index._name
        pattern = alias + '-*'

        logging.info(f'Saving template {alias} on the following pattern: {pattern}')
        index_template = cls._index.as_template(alias, pattern)
        index_template.save()

        if not cls._index.exists():
            logging.info(f'Creating index {alias + suffix}')
            es_client.indices.create(index=alias + suffix)
            es_client.indices.put_alias(index=alias + suffix, name=alias)
        else:
            logging.info(f'Index on alias {alias} already exists')

    @classmethod
    def delete_indices(cls, es_client: Elasticsearch) -> None:
        alias = cls._index._name
        pattern = alias + '*'
        logging.info(f'Deleting indices with pattern {pattern}')
        es_client.indices.delete(index=pattern)

    @classmethod
    def _matches(cls, hit):
        # override _matches to match indices in a pattern instead of just ALIAS
        # hit is the raw dict as returned by elasticsearch
        alias = cls._index._name
        pattern = alias + '-*'
        return fnmatch(hit["_index"], pattern)


class SearchableDataservice(IndexDocument):
    title = Text(analyzer=dgv_analyzer)
    created_at = Date()
    tags = Keyword(multi=True)
    organization = Keyword()
    description = Text(analyzer=dgv_analyzer)
    organization_name = Text(analyzer=dgv_analyzer)
    owner = Keyword()
    views = Float()
    followers = Float()
    description_length = Float()

    class Index:
        name = f'{Config.UDATA_INSTANCE_NAME}-dataservice'


class SearchableOrganization(IndexDocument):
    name = Text(analyzer=dgv_analyzer)
    acronym = Text()
    description = Text(analyzer=dgv_analyzer)
    url = Text()
    orga_sp = Integer()
    created_at = Date()
    followers = Float()
    views = Float()
    reuses = Float()
    datasets = Integer()
    badges = Keyword(multi=True)

    class Index:
        name = f'{Config.UDATA_INSTANCE_NAME}-organization'


class SearchableReuse(IndexDocument):
    title = Text(analyzer=dgv_analyzer)
    url = Text()
    created_at = Date()
    archived = Date()
    orga_followers = Float()
    views = Float()
    followers = Float()
    datasets = Integer()
    featured = Integer()
    type = Keyword()
    topic = Keyword()
    tags = Keyword(multi=True)
    badges = Keyword(multi=True)
    organization = Keyword()
    description = Text(analyzer=dgv_analyzer)
    organization_name = Text(analyzer=dgv_analyzer)
    organization_badges = Keyword(multi=True)
    owner = Keyword()

    class Index:
        name = f'{Config.UDATA_INSTANCE_NAME}-reuse'


class SearchableDataset(IndexDocument):
    title = Text(analyzer=dgv_analyzer)
    acronym = Text()
    url = Text()
    created_at = Date()
    last_update = Date()
    tags = Keyword(multi=True)
    license = Keyword()
    badges = Keyword(multi=True)
    frequency = Text()
    format = Keyword(multi=True)
    orga_sp = Integer()
    orga_followers = Float()
    views = Float()
    followers = Float()
    reuses = Float()
    featured = Integer()
    resources_count = Integer()
    resources_ids = Keyword(multi=True)
    resources_titles = Text(analyzer=dgv_analyzer)
    concat_title_org = Text(analyzer=dgv_analyzer)
    temporal_coverage_start = Date()
    temporal_coverage_end = Date()
    granularity = Keyword()
    geozones = Keyword(multi=True)
    description = Text(analyzer=dgv_analyzer)
    organization = Keyword()
    organization_name = Text(analyzer=dgv_analyzer)
    organization_badges = Keyword(multi=True)
    owner = Keyword()
    schema = Keyword(multi=True)
    topics = Keyword(multi=True)

    class Index:
        name = f'{Config.UDATA_INSTANCE_NAME}-dataset'


class ElasticClient:

    def __init__(self, url: str):
        self.es = connections.create_connection(hosts=[url])

    def init_indices(self) -> None:
        '''
        Create templates based on Document mappings and map patterns.
        Create time-based index matchin the template patterns.
        '''
        suffix_name = '-' + datetime.utcnow().strftime('%Y-%m-%d-%H-%M')

        SearchableDataset.init_index(self.es, suffix_name)
        SearchableReuse.init_index(self.es, suffix_name)
        SearchableOrganization.init_index(self.es, suffix_name)
        SearchableDataservice.init_index(self.es, suffix_name)

    def clean_indices(self) -> None:
        '''
        Removing previous indices and intializing new ones.
        '''

        if IS_TTY:
            msg = 'Indices will be deleted, are you sure?'
            click.confirm(msg, abort=True)
        SearchableDataset.delete_indices(self.es)
        SearchableReuse.delete_indices(self.es)
        SearchableOrganization.delete_indices(self.es)
        SearchableDataservice.delete_indices(self.es)

        self.init_indices()

    def index_organization(self, to_index: Organization, index: str = None) -> None:
        SearchableOrganization(meta={'id': to_index.id}, **to_index.to_dict()).save(skip_empty=False, index=index)

    def index_dataset(self, to_index: Dataset, index: str = None) -> None:
        SearchableDataset(meta={'id': to_index.id}, **to_index.to_dict()).save(skip_empty=False, index=index)

    def index_reuse(self, to_index: Reuse, index: str = None) -> None:
        SearchableReuse(meta={'id': to_index.id}, **to_index.to_dict()).save(skip_empty=False, index=index)

    def index_dataservice(self, to_index: Dataservice, index: str = None) -> None:
        SearchableDataservice(meta={'id': to_index.id}, **to_index.to_dict()).save(skip_empty=False, index=index)

    def query_organizations(self, query_text: str, offset: int, page_size: int, filters: dict, sort: Optional[str] = None) -> Tuple[int, List[dict]]:
        search = SearchableOrganization.search()

        for key, value in filters.items():
            search = search.filter('term', **{key: value})

        organizations_score_functions = [
            query.SF("field_value_factor", field="orga_sp", factor=8, modifier='sqrt', missing=1),
            query.SF("field_value_factor", field="followers", factor=4, modifier='sqrt', missing=1),
            query.SF("field_value_factor", field="views", factor=1, modifier='sqrt', missing=1),
        ]

        if query_text:
            search = search.query('bool', should=[
                    query.Q(
                        'function_score',
                        query=query.Bool(should=[query.MultiMatch(query=query_text, type='phrase', fields=['id^15', 'name^15', 'acronym^15', 'description^8'])]),
                        functions=organizations_score_functions
                    ),
                    query.Q(
                        'function_score',
                        query=query.Bool(should=[query.MultiMatch(
                            query=query_text,
                            type='cross_fields',
                            fields=['id^15', 'name^7', 'acronym^7', 'description^4'],
                            operator="and")]),
                        functions=organizations_score_functions
                    ),
                    query.Match(title={"query": query_text, 'fuzziness': 'AUTO:4,6'}),
            ])
        else:
            search = search.query(query.Q('function_score', query=query.MatchAll(), functions=organizations_score_functions))

        if sort:
            search = search.sort(sort, {'_score': {'order': 'desc'}})

        search = search[offset:(offset + page_size)]

        response = search.execute()
        results_number = response.hits.total.value
        if response.hits and not isinstance(response.hits[0], SearchableOrganization):
            raise ValueError(
                'Results are not of SearchableOrganization type. It probably means that index analyzers '
                'were not correctly set using template patterns on index initialization.'
            )
        res = [hit.to_dict(skip_empty=False) for hit in response.hits]
        return results_number, res

    def query_datasets(self, query_text: str, offset: int, page_size: int, filters: dict, sort: Optional[str] = None) -> Tuple[int, List[dict]]:
        search = SearchableDataset.search()

        for key, value in filters.items():
            if key == 'temporal_coverage_start':
                search = search.filter('range', **{'temporal_coverage_start': {'lte': value}})
            elif key == 'temporal_coverage_end':
                search = search.filter('range', **{'temporal_coverage_end': {'gte': value}})
            elif key == 'tags':
                # build an AND filter from tags list
                tag_filters = [query.Q('term', tags=tag) for tag in value]
                search = search.filter(
                    query.Bool(must=tag_filters)
                )
            else:
                search = search.filter('term', **{key: value})

        datasets_score_functions = [
            query.SF("field_value_factor", field="orga_sp", factor=8, modifier='sqrt', missing=1),
            query.SF("field_value_factor", field="views", factor=4, modifier='sqrt', missing=1),
            query.SF("field_value_factor", field="followers", factor=4, modifier='sqrt', missing=1),
            query.SF("field_value_factor", field="orga_followers", factor=1, modifier='sqrt', missing=1),
            query.SF("field_value_factor", field="featured", factor=1, modifier='sqrt', missing=1),
        ]

        if query_text:
            search = search.query(
                'bool',
                should=[
                    query.Q(
                        'function_score',
                        query=query.Bool(should=[query.MultiMatch(
                            query=query_text,
                            type='phrase',
                            fields=['id^15', 'title^15', 'acronym^15', 'description^8', 'organization_name^8', 'resources_ids^8', 'resources_titles^5']
                        )]),
                        functions=datasets_score_functions
                    ),
                    query.Q(
                        'function_score',
                        query=query.Bool(must=[query.Match(concat_title_org={"query": query_text, "operator": "and", "boost": 8})]),
                        functions=datasets_score_functions,
                    ),
                    query.Q(
                        'function_score',
                        query=query.Bool(should=[query.MultiMatch(
                            query=query_text,
                            type='cross_fields',
                            fields=['id^7', 'title^7', 'acronym^7', 'description^4', 'organization_name^4', 'resources_ids^4', 'resources_titles^2'],
                            operator="and")]),
                        functions=datasets_score_functions
                    ),
                    query.MultiMatch(query=query_text, type='most_fields', operator="and", fields=['title', 'organization_name'], fuzziness='AUTO:4,6')
                ])
        else:
            search = search.query(query.Q('function_score', query=query.MatchAll(), functions=datasets_score_functions))

        if sort:
            search = search.sort(sort, {'_score': {'order': 'desc'}})

        search = search[offset:(offset + page_size)]

        response = search.execute()
        results_number = response.hits.total.value
        if response.hits and not isinstance(response.hits[0], SearchableDataset):
            raise ValueError(
                'Results are not of SearchableDataset type. It probably means that index analyzers were not correctly set '
                'using template patterns on index initialization.'
            )
        res = [hit.to_dict(skip_empty=False) for hit in response.hits]
        return results_number, res

    def query_reuses(self, query_text: str, offset: int, page_size: int, filters: dict, sort: Optional[str] = None) -> Tuple[int, List[dict]]:
        search = SearchableReuse.search()

        for key, value in filters.items():
            search = search.filter('term', **{key: value})

        reuses_score_functions = [
            query.SF("field_value_factor", field="views", factor=4, modifier='sqrt', missing=1),
            query.SF("field_value_factor", field="followers", factor=4, modifier='sqrt', missing=1),
            query.SF("field_value_factor", field="orga_followers", factor=1, modifier='sqrt', missing=1),
            query.SF("field_value_factor", field="featured", factor=1, modifier='sqrt', missing=1),
            query.SF("script_score", script={"source": "doc['archived'].size() == 0 ? 1 : 0.2"}),  # score is multiplied by 0.2 for a strong malus
        ]

        if query_text:
            search = search.query('bool', should=[
                    query.Q(
                        'function_score',
                        query=query.Bool(should=[query.MultiMatch(query=query_text, type='phrase', fields=['id^15', 'title^15', 'description^8', 'organization_name^8'])]),
                        functions=reuses_score_functions
                    ),
                    query.Q(
                        'function_score',
                        query=query.Bool(should=[query.MultiMatch(
                            query=query_text,
                            type='cross_fields',
                            fields=['id^7', 'title^7', 'description^4', 'organization_name^4'],
                            operator="and")]),
                        functions=reuses_score_functions
                    ),
                    query.MultiMatch(query=query_text, type='most_fields', operator="and", fields=['title', 'organization_name'], fuzziness='AUTO:4,6')
                ])
        else:
            search = search.query(query.Q('function_score', query=query.MatchAll(), functions=reuses_score_functions))

        if sort:
            search = search.sort(sort, {'_score': {'order': 'desc'}})

        search = search[offset:(offset + page_size)]

        response = search.execute()
        results_number = response.hits.total.value
        if response.hits and not isinstance(response.hits[0], SearchableReuse):
            raise ValueError(
                'Results are not of SearchableReuse type. It probably means that index analyzers were not correctly set '
                'using template patterns on index initialization.'
            )
        res = [hit.to_dict(skip_empty=False) for hit in response.hits]
        return results_number, res

    def query_dataservices(self, query_text: str, offset: int, page_size: int, filters: dict, sort: Optional[str] = None) -> Tuple[int, List[dict]]:
        search = SearchableDataservice.search()

        for key, value in filters.items():
            search = search.filter('term', **{key: value})

        dataservices_score_functions = [
            query.SF("field_value_factor", field="description_length", factor=1, modifier='sqrt', missing=1),
            query.SF("field_value_factor", field="views", factor=4, modifier='sqrt', missing=1),
            query.SF("field_value_factor", field="followers", factor=4, modifier='sqrt', missing=1),
            query.SF("field_value_factor", field="orga_followers", factor=1, modifier='sqrt', missing=1),
        ]

        if query_text:
            search = search.query('bool', should=[
                    query.Q(
                        'function_score',
                        query=query.Bool(should=[query.MultiMatch(query=query_text, type='phrase', fields=['id^15', 'title^15', 'description^8', 'organization_name^8'])]),
                        functions=dataservices_score_functions
                    ),
                    query.Q(
                        'function_score',
                        query=query.Bool(should=[query.MultiMatch(
                            query=query_text,
                            type='cross_fields',
                            fields=['id^7', 'title^7', 'description^4', 'organization_name^4'],
                            operator="and")]),
                        functions=dataservices_score_functions
                    ),
                    query.MultiMatch(query=query_text, type='most_fields', operator="and", fields=['title', 'organization_name'], fuzziness='AUTO:4,6')
                ])
        else:
            search = search.query(query.Q('function_score', query=query.MatchAll(), functions=dataservices_score_functions))

        if sort:
            search = search.sort(sort, {'_score': {'order': 'desc'}})

        search = search[offset:(offset + page_size)]

        response = search.execute()
        results_number = response.hits.total.value
        if response.hits and not isinstance(response.hits[0], SearchableDataservice):
            raise ValueError(
                'Results are not of SearchableDataservice type. It probably means that index analyzers were not correctly set '
                'using template patterns on index initialization.'
            )
        res = [hit.to_dict(skip_empty=False) for hit in response.hits]
        return results_number, res

    def find_one_organization(self, organization_id: str) -> Optional[dict]:
        try:
            return SearchableOrganization.get(id=organization_id).to_dict()
        except NotFoundError:
            return None

    def find_one_dataset(self, dataset_id: str) -> Optional[dict]:
        try:
            return SearchableDataset.get(id=dataset_id).to_dict()
        except NotFoundError:
            return None

    def find_one_reuse(self, reuse_id: str) -> Optional[dict]:
        try:
            return SearchableReuse.get(id=reuse_id).to_dict()
        except NotFoundError:
            return None

    def find_one_dataservice(self, dataservice_id: str) -> Optional[dict]:
        try:
            return SearchableDataservice.get(id=dataservice_id).to_dict()
        except NotFoundError:
            return None

    def delete_one_organization(self, organization_id: str) -> Optional[str]:
        try:
            SearchableOrganization.get(id=organization_id).delete()
            return organization_id
        except NotFoundError:
            return None

    def delete_one_dataset(self, dataset_id: str) -> Optional[str]:
        try:
            SearchableDataset.get(id=dataset_id).delete()
            return dataset_id
        except NotFoundError:
            return None

    def delete_one_reuse(self, reuse_id: str) -> Optional[str]:
        try:
            SearchableReuse.get(id=reuse_id).delete()
            return reuse_id
        except NotFoundError:
            return None

    def delete_one_dataservice(self, dataservice_id: str) -> Optional[str]:
        try:
            SearchableDataservice.get(id=dataservice_id).delete()
            return dataservice_id
        except NotFoundError:
            return None
