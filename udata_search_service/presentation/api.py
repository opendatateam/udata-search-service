import logging
import re
from typing import Optional
from dependency_injector.wiring import inject, Provide
from flask import Blueprint, request, url_for, jsonify, abort
from pydantic import BaseModel, Field, ValidationError, validator
from udata_search_service.container import Container
from udata_search_service.config import Config
from udata_search_service.infrastructure.services import DatasetService, OrganizationService, ReuseService, DataserviceService
from udata_search_service.infrastructure.search_clients import ElasticClient
from udata_search_service.infrastructure.consumers import DatasetConsumer, ReuseConsumer, OrganizationConsumer, DataserviceConsumer
from udata_search_service.infrastructure.migrate import set_alias as set_alias_func
from udata_search_service.presentation.utils import is_list_type


bp = Blueprint('api', __name__, url_prefix='/api/1')


class OrganizationArgs(BaseModel):
    q: Optional[str] = None
    page: Optional[int] = 1
    page_size: Optional[int] = 20
    sort: Optional[str] = None
    badge: Optional[str] = None

    @validator('sort')
    def sort_validate(cls, value):
        sorts = [
            'created', 'reuses', 'datasets', 'followers', 'views'
        ]
        choices = sorts + ['-' + k for k in sorts]
        if value not in choices:
            raise ValueError('Sort parameter is not in the sorts available choices.')
        return value


class DatasetArgs(BaseModel):
    q: Optional[str] = None
    page: Optional[int] = 1
    page_size: Optional[int] = 20
    sort: Optional[str] = None
    tag: Optional[list[str]] = None
    badge: Optional[str] = None
    organization: Optional[str] = None
    organization_badge: Optional[str] = None
    owner: Optional[str] = None
    license: Optional[str] = None
    geozone: Optional[str] = None
    granularity: Optional[str] = None
    format: Optional[str] = None
    temporal_coverage: Optional[str] = None
    featured: Optional[str] = None
    schema_: Optional[str] = Field(None, alias="schema")
    topic: Optional[str] = None

    @validator('temporal_coverage')
    def temporal_coverage_format(cls, value):
        pattern = re.compile("^([0-9]{4}-[0-9]{2}-[0-9]{2}-[0-9]{4}-[0-9]{2}-[0-9]{2})$")
        if not pattern.match(value):
            raise ValueError('Temporal coverage does not match the right pattern.')
        return value

    @validator('sort')
    def sort_validate(cls, value):
        sorts = [
            'created', 'last_update', 'reuses', 'followers', 'views'
        ]
        choices = sorts + ['-' + k for k in sorts]
        if value not in choices:
            raise ValueError('Temporal coverage does not match the right pattern.')
        return value

    @classmethod
    def from_request_args(cls, request_args) -> 'DatasetArgs':
        def get_list_args() -> dict:
            return {
                key: value
                for key, value in request_args.to_dict(flat=False).items()
                if key in cls.__fields__
                and is_list_type(cls.__fields__[key].annotation)
            }

        return cls(
            **{
                **request_args.to_dict(),
                **get_list_args(),
            }
        )

class ReuseArgs(BaseModel):
    q: Optional[str] = None
    page: Optional[int] = 1
    page_size: Optional[int] = 20
    sort: Optional[str] = None
    tag: Optional[str] = None
    badge: Optional[str] = None
    organization: Optional[str] = None
    organization_badge: Optional[str] = None
    owner: Optional[str] = None
    type: Optional[str] = None
    featured: Optional[str] = None
    topic: Optional[str] = None

    @validator('sort')
    def sort_validate(cls, value):
        sorts = [
            'created', 'datasets', 'followers', 'views'
        ]
        choices = sorts + ['-' + k for k in sorts]
        if value not in choices:
            raise ValueError('Temporal coverage does not match the right pattern.')
        return value


class DataserviceArgs(BaseModel):
    q: Optional[str] = None
    page: Optional[int] = 1
    page_size: Optional[int] = 20
    sort: Optional[str] = None
    tag: Optional[str] = None
    organization: Optional[str] = None
    owner: Optional[str] = None
    is_restricted: Optional[bool] = None

    @validator('sort')
    def sort_validate(cls, value):
        sorts = [
            'created', 'followers', 'views'
        ]
        choices = sorts + ['-' + k for k in sorts]
        if value not in choices:
            raise ValueError('Sort parameter is not in the sorts available choices.')
        return value


def make_response(results, total_pages, results_number, page, page_size, next_url, prev_url):
    return jsonify({
        "data": results,
        "next_page": next_url if page < total_pages else None,
        "page": page,
        "previous_page": prev_url if page > 1 else None,
        "page_size": page_size,
        "total_pages": total_pages,
        "total": results_number
    })


class DatasetToIndex(BaseModel):
    id: str
    title: str
    description: str
    acronym: Optional[str] = None
    url: Optional[str] = None
    tags: Optional[list] = []
    license: Optional[str] = None
    badges: Optional[list] = []
    frequency: Optional[str] = None
    created_at: str
    last_update: str
    organization: Optional[dict] = None
    owner: Optional[str] = None
    views: int
    followers: int
    reuses: int
    resources_count: Optional[int] = 0
    resources: Optional[list] = None
    featured: Optional[int] = 0
    format: Optional[list] = []
    schema_: Optional[list] = Field([], alias="schema")
    extras: Optional[dict] = {}
    harvest: Optional[dict] = {}
    temporal_coverage_start: Optional[str] = None
    temporal_coverage_end: Optional[str] = None
    geozones: Optional[list] = []
    granularity: Optional[str] = None
    topics: Optional[list] = []


class RequestDatasetIndex(BaseModel):
    document: DatasetToIndex
    index: Optional[str] = None


@bp.route("/datasets/index", methods=["POST"], endpoint='dataset_index')
@inject
def dataset_index(dataset_service: DatasetService = Provide[Container.dataset_service], search_client: ElasticClient = Provide[Container.search_client]):
    try:
        validated_obj = RequestDatasetIndex(**request.json)
    except ValidationError as e:
        abort(400, e)

    document = DatasetConsumer.load_from_dict(validated_obj.document.dict())
    index_name = f'{Config.UDATA_INSTANCE_NAME}-{validated_obj.index}' if validated_obj.index else None
    if index_name and not search_client.es.indices.exists(index=index_name):
        abort(404, 'Index does not exist')
    dataset_service.feed(document, index_name)
    return jsonify({'data': 'Dataset added to index'})


@bp.route("/datasets/<dataset_id>/unindex", methods=["DELETE"], endpoint='dataset_unindex')
@inject
def dataset_unindex(dataset_id: str, dataset_service: DatasetService = Provide[Container.dataset_service]):
    result = dataset_service.delete_one(dataset_id)
    if result:
        return jsonify({'data': f'Dataset {result} removed from index'})
    abort(404, 'reuse not found')


@bp.route("/datasets/", methods=["GET"], endpoint='dataset_search')
@inject
def datasets_search(dataset_service: DatasetService = Provide[Container.dataset_service]):
    try:
        request_args = DatasetArgs.from_request_args(request.args)
    except ValidationError as e:
        abort(400, e)

    results, results_number, total_pages = dataset_service.search(request_args.dict())

    next_url = url_for('api.dataset_search', q=request_args.q, page=request_args.page + 1,
                       page_size=request_args.page_size, _external=True)
    prev_url = url_for('api.dataset_search', q=request_args.q, page=request_args.page - 1,
                       page_size=request_args.page_size, _external=True)

    return make_response(results, total_pages, results_number,
                         request_args.page, request_args.page_size, next_url, prev_url)


@bp.route("/datasets/<dataset_id>/", methods=["GET"], endpoint='dataset_get_specific')
@inject
def get_dataset(dataset_id: str, dataset_service: DatasetService = Provide[Container.dataset_service]):
    result = dataset_service.find_one(dataset_id)
    if result:
        return jsonify(result)
    abort(404, 'dataset not found')


@bp.route("/organizations/", methods=["GET"], endpoint='organization_search')
@inject
def organizations_search(organization_service: OrganizationService = Provide[Container.organization_service]):
    try:
        request_args = OrganizationArgs(**request.args)
    except ValidationError as e:
        abort(400, e)

    results, results_number, total_pages = organization_service.search(request_args.dict())

    next_url = url_for('api.organization_search', q=request_args.q, page=request_args.page + 1,
                       page_size=request_args.page_size, _external=True)
    prev_url = url_for('api.organization_search', q=request_args.q, page=request_args.page - 1,
                       page_size=request_args.page_size, _external=True)

    return make_response(results, total_pages, results_number, request_args.page,
                         request_args.page_size, next_url, prev_url)


@bp.route("/organizations/<organization_id>/", methods=["GET"], endpoint='organization_get_specific')
@inject
def get_organization(organization_id: str,
                     organization_service: OrganizationService = Provide[Container.organization_service]):
    result = organization_service.find_one(organization_id)
    if result:
        return jsonify(result)
    abort(404, 'organization not found')


class OrganizationToIndex(BaseModel):
    id: str
    name: str
    description: str
    acronym: Optional[str] = None
    url: Optional[str] = None
    badges: Optional[list] = []
    created_at: str
    orga_sp: int
    datasets: int
    followers: int
    reuses: int
    views: int
    extras: Optional[dict] = {}


class RequestOrganizationIndex(BaseModel):
    document: OrganizationToIndex
    index: Optional[str] = None


@bp.route("/organizations/index", methods=["POST"], endpoint='organization_index')
@inject
def organization_index(organization_service: OrganizationService = Provide[Container.organization_service], search_client: ElasticClient = Provide[Container.search_client]):
    try:
        validated_obj = RequestOrganizationIndex(**request.json)
    except ValidationError as e:
        abort(400, e)

    document = OrganizationConsumer.load_from_dict(validated_obj.document.dict())
    index_name = f'{Config.UDATA_INSTANCE_NAME}-{validated_obj.index}' if validated_obj.index else None
    if index_name and not search_client.es.indices.exists(index=index_name):
        abort(404, 'Index does not exist')
    organization_service.feed(document, index_name)
    return jsonify({'data': 'Organization added to index'})


@bp.route("/organizations/<organization_id>/unindex", methods=["DELETE"], endpoint='organization_unindex')
@inject
def organization_unindex(organization_id: str, organization_service: OrganizationService = Provide[Container.organization_service]):
    result = organization_service.delete_one(organization_id)
    if result:
        return jsonify({'data': f'Organization {result} removed from index'})
    abort(404, 'reuse not found')


@bp.route("/reuses/", methods=["GET"], endpoint='reuse_search')
@inject
def reuses_search(reuse_service: ReuseService = Provide[Container.reuse_service]):
    try:
        request_args = ReuseArgs(**request.args)
    except ValidationError as e:
        abort(400, e)

    results, results_number, total_pages = reuse_service.search(request_args.dict())

    next_url = url_for('api.reuse_search', q=request_args.q, page=request_args.page + 1,
                       page_size=request_args.page_size, _external=True)
    prev_url = url_for('api.reuse_search', q=request_args.q, page=request_args.page - 1,
                       page_size=request_args.page_size, _external=True)

    return make_response(results, total_pages, results_number, request_args.page,
                         request_args.page_size, next_url, prev_url)


@bp.route("/reuses/<reuse_id>/", methods=["GET"], endpoint='reuse_get_specific')
@inject
def get_reuse(reuse_id: str, reuse_service: ReuseService = Provide[Container.reuse_service]):
    result = reuse_service.find_one(reuse_id)
    if result:
        return jsonify(result)
    abort(404, 'reuse not found')


class ReuseToIndex(BaseModel):
    id: str
    title: str
    description: str
    url: Optional[str] = None
    badges: Optional[list] = []
    created_at: str
    archived: Optional[str] = None
    datasets: int
    followers: int
    views: int
    featured: int
    organization: Optional[dict] = {}
    owner: Optional[str] = None
    type: str
    topic: str
    tags: Optional[list] = []
    extras: Optional[dict] = {}


class RequestReuseIndex(BaseModel):
    document: ReuseToIndex
    index: Optional[str] = None


@bp.route("/reuses/index", methods=["POST"], endpoint='reuse_index')
@inject
def reuse_index(reuse_service: ReuseService = Provide[Container.reuse_service], search_client: ElasticClient = Provide[Container.search_client]):
    try:
        validated_obj = RequestReuseIndex(**request.json)
    except ValidationError as e:
        abort(400, e)

    document = ReuseConsumer.load_from_dict(validated_obj.document.dict())
    index_name = f'{Config.UDATA_INSTANCE_NAME}-{validated_obj.index}' if validated_obj.index else None
    if index_name and not search_client.es.indices.exists(index=index_name):
        abort(404, 'Index does not exist')

    reuse_service.feed(document, index_name)
    return jsonify({'data': 'Reuse added to index'})


@bp.route("/reuses/<reuse_id>/unindex", methods=["DELETE"], endpoint='reuse_unindex')
@inject
def reuse_unindex(reuse_id: str, reuse_service: ReuseService = Provide[Container.reuse_service]):
    result = reuse_service.delete_one(reuse_id)
    if result:
        return jsonify({'data': f'Reuse {result} removed from index'})
    abort(404, 'reuse not found')


@bp.route("/dataservices/", methods=["GET"], endpoint='dataservice_search')
@inject
def dataservices_search(dataservice_service: DataserviceService = Provide[Container.dataservice_service]):
    try:
        request_args = DataserviceArgs(**request.args)
    except ValidationError as e:
        abort(400, e)

    results, results_number, total_pages = dataservice_service.search(request_args.dict())

    next_url = url_for('api.dataservice_search', q=request_args.q, page=request_args.page + 1,
                       page_size=request_args.page_size, _external=True)
    prev_url = url_for('api.dataservice_search', q=request_args.q, page=request_args.page - 1,
                       page_size=request_args.page_size, _external=True)

    return make_response(results, total_pages, results_number, request_args.page,
                         request_args.page_size, next_url, prev_url)


@bp.route("/dataservices/<dataservice_id>/", methods=["GET"], endpoint='dataservice_get_specific')
@inject
def get_dataservice(dataservice_id: str, dataservice_service: DataserviceService = Provide[Container.dataservice_service]):
    result = dataservice_service.find_one(dataservice_id)
    if result:
        return jsonify(result)
    abort(404, 'dataservice not found')


class DataserviceToIndex(BaseModel):
    id: str
    title: str
    description: str
    created_at: str
    views: int = 0
    followers: int = 0
    organization: Optional[dict] = {}
    owner: Optional[str] = None
    tags: Optional[list] = []
    extras: Optional[dict] = {}
    is_restricted: Optional[bool] = None


class RequestDataserviceIndex(BaseModel):
    document: DataserviceToIndex
    index: Optional[str] = None


@bp.route("/dataservices/index", methods=["POST"], endpoint='dataservice_index')
@inject
def dataservice_index(dataservice_service: DataserviceService = Provide[Container.dataservice_service], search_client: ElasticClient = Provide[Container.search_client]):
    try:
        validated_obj = RequestDataserviceIndex(**request.json)
    except ValidationError as e:
        abort(400, e)

    document = DataserviceConsumer.load_from_dict(validated_obj.document.dict())
    index_name = f'{Config.UDATA_INSTANCE_NAME}-{validated_obj.index}' if validated_obj.index else None
    if index_name and not search_client.es.indices.exists(index=index_name):
        abort(404, 'Index does not exist')

    dataservice_service.feed(document, index_name)
    return jsonify({'data': 'Dataservice added to index'})


@bp.route("/dataservices/<dataservice_id>/unindex", methods=["DELETE"], endpoint='dataservice_unindex')
@inject
def dataservice_unindex(dataservice_id: str, dataservice_service: DataserviceService = Provide[Container.dataservice_service]):
    result = dataservice_service.delete_one(dataservice_id)
    if result:
        return jsonify({'data': f'Dataservice {result} removed from index'})
    abort(404, 'dataservice not found')


@bp.route("/create-index", methods=["POST"], endpoint='create_index')
@inject
def create_index(search_client: ElasticClient = Provide[Container.search_client]):
    try:
        index_name = request.json['index']
    except KeyError:
        abort(400, 'Missing index in payload')

    index_name = f'{Config.UDATA_INSTANCE_NAME}-{index_name}'
    # Initiliaze index matching template pattern
    if not search_client.es.indices.exists(index=index_name):
        logging.info(f'Initializing new index {index_name} for reindexation')
        search_client.es.indices.create(index=index_name)
        # Check whether template with analyzer was correctly assigned
        if 'analysis' not in search_client.es.indices.get_settings(index=index_name)[index_name]['settings']['index']:
            logging.error(f'Analyzer was not set using templates when initializing {index_name}')
        return jsonify({'data': f'Index {index_name} created'})
    return jsonify({'data': f'Index {index_name} already exists'})


class RequestSetAlias(BaseModel):
    index_suffix_name: str
    indices: Optional[list] = []


@bp.route("/set-index-alias", methods=["POST"], endpoint='set_index_alias')
@inject
def set_index_alias(search_client: ElasticClient = Provide[Container.search_client]):
    try:
        validated_obj = RequestSetAlias(**request.json).dict()
    except ValidationError as e:
        abort(400, e)

    indices = [index.lower().rstrip('s') for index in (validated_obj['indices'] or [])]
    set_alias_func(search_client, validated_obj['index_suffix_name'], indices)
    return jsonify({'data': 'Alias set'})
