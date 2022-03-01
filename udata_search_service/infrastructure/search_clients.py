from datetime import datetime
from fnmatch import fnmatch
from typing import Tuple, Optional, List

from elasticsearch.exceptions import NotFoundError
from elasticsearch_dsl import Index, Date, Document, Float, Integer, Keyword, Text, tokenizer, token_filter, analyzer, query
from elasticsearch_dsl.connections import connections
from udata_search_service.domain.entities import Dataset, Organization, Reuse
from udata_search_service.config import Config


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


class SearchableOrganization(Document):
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

    @classmethod
    def _matches(cls, hit):
        # override _matches to match indices in a pattern instead of just ALIAS
        # hit is the raw dict as returned by elasticsearch
        return fnmatch(hit["_index"], 'organization-*')

    class Index:
        name = 'organization'


class SearchableReuse(Document):
    title = Text(analyzer=dgv_analyzer)
    url = Text()
    created_at = Date()
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
    owner = Keyword()

    @classmethod
    def _matches(cls, hit):
        # override _matches to match indices in a pattern instead of just ALIAS
        # hit is the raw dict as returned by elasticsearch
        return fnmatch(hit["_index"], 'reuse-*')

    class Index:
        name = 'reuse'


class SearchableDataset(Document):
    title = Text(analyzer=dgv_analyzer)
    acronym = Text()
    url = Text()
    created_at = Date()
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
    concat_title_org = Text(analyzer=dgv_analyzer)
    temporal_coverage_start = Date()
    temporal_coverage_end = Date()
    granularity = Keyword()
    geozones = Keyword(multi=True)
    description = Text(analyzer=dgv_analyzer)
    organization = Keyword()
    organization_name = Text(analyzer=dgv_analyzer)
    owner = Keyword()
    schema = Keyword(multi=True)

    @classmethod
    def _matches(cls, hit):
        # override _matches to match indices in a pattern instead of just ALIAS
        # hit is the raw dict as returned by elasticsearch
        return fnmatch(hit["_index"], 'dataset-*')

    class Index:
        name = 'dataset'


class ElasticClient:

    def __init__(self, url: str):
        self.es = connections.create_connection(hosts=[url])

    def delete_index_with_alias(self, alias: str) -> None:
        if self.es.indices.exists_alias(name=alias):
            for previous_index in self.es.indices.get_alias(alias).keys():
                Index(previous_index).delete()

    def init_indices(self) -> None:
        '''
        Create templates based on Document mappings and map patterns.
        Create time-based index matchin the template patterns.
        '''
        suffix_name = '-' + datetime.now().strftime('%Y-%m-%d-%H-%M')

        index_template = SearchableDataset._index.as_template('dataset', 'dataset-*')
        index_template.save()
        self.es.indices.create(index='dataset' + suffix_name)

        index_template = SearchableReuse._index.as_template('reuse', 'reuse-*')
        index_template.save()
        self.es.indices.create(index='reuse' + suffix_name)

        index_template = SearchableOrganization._index.as_template('organization', 'organization-*')
        index_template.save()
        self.es.indices.create(index='organization' + suffix_name)

        self.clean_aliases(suffix_name)

    def clean_indices(self) -> None:
        for alias in ['dataset', 'reuse', 'organization']:
            self.delete_index_with_alias(alias)

        self.init_indices()

    def clean_aliases(self, suffix: str) -> None:
        for alias in ['dataset', 'reuse', 'organization']:
            pattern = alias + '-*'
            self.es.indices.update_aliases(
                body={
                    "actions": [
                        {"remove": {"alias": alias, "index": pattern}},
                        {"add": {"alias": alias, "index": alias + suffix}},
                    ]
                }
            )

    def index_organization(self, to_index: Organization) -> None:
        SearchableOrganization(meta={'id': to_index.id}, **to_index.to_dict()).save(skip_empty=False)

    def index_dataset(self, to_index: Dataset) -> None:
        SearchableDataset(meta={'id': to_index.id}, **to_index.to_dict()).save(skip_empty=False)

    def index_reuse(self, to_index: Reuse) -> None:
        SearchableReuse(meta={'id': to_index.id}, **to_index.to_dict()).save(skip_empty=False)

    def query_organizations(self, query_text: str, offset: int, page_size: int, filters: dict, sort: Optional[str] = None) -> Tuple[int, List[dict]]:
        s = SearchableOrganization.search()

        for key, value in filters.items():
            s = s.filter('term', **{key: value})

        organizations_score_functions = [
            query.SF("field_value_factor", field="orga_sp", factor=8, modifier='sqrt', missing=1),
            query.SF("field_value_factor", field="followers", factor=4, modifier='sqrt', missing=1),
            query.SF("field_value_factor", field="datasets", factor=1, modifier='sqrt', missing=1),
        ]

        if query_text:
            s = s.query('bool', should=[
                    query.Q(
                        'function_score',
                        query=query.Bool(should=[query.MultiMatch(query=query_text, type='phrase', fields=['name^15', 'acronym^15', 'description^8'])]),
                        functions=organizations_score_functions
                    ),
                query.Match(title={"query": query_text, 'fuzziness': 'AUTO:4,6'})
            ])
        else:
            s = s.query(query.Q('function_score', query=query.MatchAll(), functions=organizations_score_functions))

        if sort:
            s = s.sort(sort, {'_score': {'order': 'desc'}})

        s = s[offset:(offset + page_size)]

        response = s.execute()
        results_number = response.hits.total.value
        res = [hit.to_dict(skip_empty=False) for hit in response.hits]
        return results_number, res

    def query_datasets(self, query_text: str, offset: int, page_size: int, filters: dict, sort: Optional[str] = None) -> Tuple[int, List[dict]]:
        s = SearchableDataset.search()

        for key, value in filters.items():
            if key == 'temporal_coverage_start':
                s = s.filter('range', **{'temporal_coverage_start': {'lte': value}})
            elif key == 'temporal_coverage_end':
                s = s.filter('range', **{'temporal_coverage_end': {'gte': value}})
            else:
                s = s.filter('term', **{key: value})

        datasets_score_functions = [
            query.SF("field_value_factor", field="orga_sp", factor=8, modifier='sqrt', missing=1),
            query.SF("field_value_factor", field="views", factor=4, modifier='sqrt', missing=1),
            query.SF("field_value_factor", field="followers", factor=4, modifier='sqrt', missing=1),
            query.SF("field_value_factor", field="orga_followers", factor=1, modifier='sqrt', missing=1),
            query.SF("field_value_factor", field="featured", factor=1, modifier='sqrt', missing=1),
        ]

        if query_text:
            s = s.query(
                'bool',
                should=[
                    query.Q(
                        'function_score',
                        query=query.Bool(should=[query.MultiMatch(query=query_text, type='phrase', fields=['title^15', 'acronym^15', 'description^8', 'organization_name^8'])]),
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
                            fields=['title^7', 'acronym^7', 'description^4', 'organization_name^4'],
                            operator="and")]),
                        functions=datasets_score_functions
                    ),
                    query.MultiMatch(query=query_text, type='most_fields', operator="and", fields=['title', 'organization_name'], fuzziness='AUTO:4,6')
                ])
        else:
            s = s.query(query.Q('function_score', query=query.MatchAll(), functions=datasets_score_functions))

        if sort:
            s = s.sort(sort, {'_score': {'order': 'desc'}})

        s = s[offset:(offset + page_size)]

        response = s.execute()
        results_number = response.hits.total.value
        res = [hit.to_dict(skip_empty=False) for hit in response.hits]
        return results_number, res

    def query_reuses(self, query_text: str, offset: int, page_size: int, filters: dict, sort: Optional[str] = None) -> Tuple[int, List[dict]]:
        s = SearchableReuse.search()

        for key, value in filters.items():
            s = s.filter('term', **{key: value})

        reuses_score_functions = [
            query.SF("field_value_factor", field="views", factor=4, modifier='sqrt', missing=1),
            query.SF("field_value_factor", field="followers", factor=4, modifier='sqrt', missing=1),
            query.SF("field_value_factor", field="orga_followers", factor=1, modifier='sqrt', missing=1),
            query.SF("field_value_factor", field="featured", factor=1, modifier='sqrt', missing=1),
        ]

        if query_text:
            s = s.query('bool', should=[
                    query.Q(
                        'function_score',
                        query=query.Bool(should=[query.MultiMatch(query=query_text, type='phrase', fields=['title^15', 'description^8', 'organization_name^8'])]),
                        functions=reuses_score_functions
                    ),
                    query.MultiMatch(query=query_text, type='most_fields', fields=['title', 'organization_name'], fuzziness='AUTO:4,6')
                ])
        else:
            s = s.query(query.Q('function_score', query=query.MatchAll(), functions=reuses_score_functions))

        if sort:
            s = s.sort(sort, {'_score': {'order': 'desc'}})

        s = s[offset:(offset + page_size)]

        response = s.execute()
        results_number = response.hits.total.value
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
