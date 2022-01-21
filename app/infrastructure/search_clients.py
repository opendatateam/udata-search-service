from typing import Tuple, Optional, List
from elasticsearch.exceptions import NotFoundError
from elasticsearch_dsl import Index, Document, Integer, Text, tokenizer, token_filter, analyzer, query, Date
from elasticsearch_dsl.connections import connections
from app.domain.entities import Dataset, Organization, Reuse
from app.config import Config


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
    followers = Integer()
    datasets = Integer()

    class Index:
        name = 'organization'


class SearchableReuse(Document):
    title = Text(analyzer=dgv_analyzer)
    url = Text()
    created_at = Date()
    orga_followers = Integer()
    views = Integer()
    followers = Integer()
    datasets = Integer()
    featured = Integer()
    organization_id = Text()
    description = Text(analyzer=dgv_analyzer)
    organization = Text(analyzer=dgv_analyzer)

    class Index:
        name = 'reuse'


class SearchableDataset(Document):
    title = Text(analyzer=dgv_analyzer)
    acronym = Text()
    url = Text()
    created_at = Date()
    orga_sp = Integer()
    orga_followers = Integer()
    views = Integer()
    followers = Integer()
    reuses = Integer()
    featured = Integer()
    resources_count = Integer()
    concat_title_org = Text(analyzer=dgv_analyzer)
    organization_id = Text()
    temporal_coverage_start = Date()
    temporal_coverage_end = Date()
    granularity = Text()
    geozones = Text()
    description = Text(analyzer=dgv_analyzer)
    organization = Text(analyzer=dgv_analyzer)

    class Index:
        name = 'dataset'


class ElasticClient:

    def __init__(self, url: str):
        connections.create_connection(hosts=[url])

    def clean_indices(self) -> None:
        if Index('dataset').exists():
            Index('dataset').delete()
        SearchableDataset.init()
        if Index('reuse').exists():
            Index('reuse').delete()
        SearchableReuse.init()
        if Index('organization').exists():
            Index('organization').delete()
        SearchableOrganization.init()

    def index_organization(self, to_index: Organization) -> None:
        SearchableOrganization(meta={'id': to_index.id}, **to_index.to_dict()).save(skip_empty=False)

    def index_dataset(self, to_index: Dataset) -> None:
        SearchableDataset(meta={'id': to_index.id}, **to_index.to_dict()).save(skip_empty=False)

    def index_reuse(self, to_index: Reuse) -> None:
        SearchableReuse(meta={'id': to_index.id}, **to_index.to_dict()).save(skip_empty=False)

    def query_organizations(self, query_text: str, offset: int, page_size: int) -> Tuple[int, List[dict]]:
        s = SearchableOrganization.search().query('bool', should=[
                query.Q(
                    'function_score',
                    query=query.Bool(should=[query.MultiMatch(query=query_text, type='phrase', fields=['title^15','acronym^15','description^8'])]),
                    functions=[
                        query.SF("field_value_factor", field="orga_sp", factor=8, modifier='sqrt', missing=1),
                        query.SF("field_value_factor", field="followers", factor=4, modifier='sqrt', missing=1),
                        query.SF("field_value_factor", field="datasets", factor=1, modifier='sqrt', missing=1),
                    ],
                ),
            query.Match(title={"query": query_text, 'fuzziness': 'AUTO'})
        ])
        s = s[offset:(offset + page_size)]
        response = s.execute()
        results_number = response.hits.total.value
        res = [hit.to_dict(skip_empty=False) for hit in response.hits]
        return results_number, res

    def query_datasets(self, query_text: str, offset: int, page_size: int) -> Tuple[int, List[dict]]:
        datasets_score_functions = [
            query.SF("field_value_factor", field="orga_sp", factor=8, modifier='sqrt', missing=1),
            query.SF("field_value_factor", field="views", factor=4, modifier='sqrt', missing=1),
            query.SF("field_value_factor", field="followers", factor=4, modifier='sqrt', missing=1),
            query.SF("field_value_factor", field="orga_followers", factor=1, modifier='sqrt', missing=1),
            query.SF("field_value_factor", field="featured", factor=1, modifier='sqrt', missing=1),
        ]
        s = SearchableDataset.search().query(
            'bool',
            should=[
                query.Q(
                    'function_score',
                    query=query.Bool(should=[query.MultiMatch(query=query_text, type='phrase', fields=['title^15','acronym^15','description^8','organization^8'])]),
                    functions=datasets_score_functions,
                ),
                query.Q(
                    'function_score',
                    query=query.Bool(must=[query.Match(concat_title_org={"query": query_text, "operator": "and", "boost": 8})]),
                    functions=datasets_score_functions,
                ),
                query.MultiMatch(query=query_text, type='most_fields', fields=['title', 'organization'], fuzziness='AUTO')
            ])
        s = s[offset:(offset + page_size)]
        response = s.execute()
        results_number = response.hits.total.value
        res = [hit.to_dict(skip_empty=False) for hit in response.hits]
        return results_number, res

    def query_reuses(self, query_text: str, offset: int, page_size: int) -> Tuple[int, List[dict]]:
        s = SearchableReuse.search().query('bool', should=[
                query.Q(
                    'function_score',
                    query=query.Bool(should=[query.MultiMatch(query=query_text, type='phrase', fields=['title^15','description^8','organization^8'])]),
                    functions=[
                        query.SF("field_value_factor", field="views", factor=4, modifier='sqrt', missing=1),
                        query.SF("field_value_factor", field="followers", factor=4, modifier='sqrt', missing=1),
                        query.SF("field_value_factor", field="orga_followers", factor=1, modifier='sqrt', missing=1),
                        query.SF("field_value_factor", field="featured", factor=1, modifier='sqrt', missing=1),
                    ],
                ),
                query.MultiMatch(query=query_text, type='most_fields', fields=['title', 'organization'], fuzziness='AUTO')
            ])
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
