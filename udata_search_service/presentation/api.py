import logging
import re
from typing import Optional
from elasticsearch import Elasticsearch
from dependency_injector.wiring import inject, Provide
from flask import Blueprint, request, url_for, jsonify, abort
from pydantic import BaseModel, Field, ValidationError, validator
from udata_search_service.container import Container
from udata_search_service.config import Config
from udata_search_service.infrastructure.services import DatasetService, OrganizationService, ReuseService
from udata_search_service.infrastructure.consumers import DatasetConsumer, ReuseConsumer, OrganizationConsumer, EventMessageType, parse_message


bp = Blueprint('api', __name__, url_prefix='/api/1')


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


class DatasetToIndex(BaseModel):
    id: str
    title: str
    description: str
    acronym: Optional[str] = None
    url: Optional[str] = None
    tags: Optional[list] = None
    license: Optional[str] = None
    badges: Optional[list] = None
    frequency: Optional[str] = None
    created_at: str
    organization: Optional[dict] = None
    owner: Optional[str] = None
    views: int
    followers: int
    reuses: int
    resources_count: Optional[int] = 0
    featured: Optional[int] = 0
    format: Optional[list] = None
    schema_: Optional[dict] = Field(None, alias="schema")
    extras: Optional[dict] = None
    harvest: Optional[dict] = None
    temporal_coverage_start: Optional[str] = None
    temporal_coverage_end: Optional[str] = None
    geozones: Optional[list] = None
    granularity: Optional[str] = None


class DatasetArgs(BaseModel):
    q: Optional[str] = None
    page: Optional[int] = 1
    page_size: Optional[int] = 20
    sort: Optional[str] = None
    tag: Optional[str] = None
    badge: Optional[str] = None
    organization: Optional[str] = None
    owner: Optional[str] = None
    license: Optional[str] = None
    geozone: Optional[str] = None
    granularity: Optional[str] = None
    format: Optional[str] = None
    temporal_coverage: Optional[str] = None
    featured: Optional[str] = None
    schema_: Optional[str] = Field(None, alias="schema")

    @validator('temporal_coverage')
    def temporal_coverage_format(cls, value):
        pattern = re.compile("^([0-9]{4}-[0-9]{2}-[0-9]{2}-[0-9]{4}-[0-9]{2}-[0-9]{2})$")
        if not pattern.match(value):
            raise ValueError('Temporal coverage does not match the right pattern.')
        return value

    @validator('sort')
    def sort_validate(cls, value):
        sorts = [
            'created', 'reuses', 'followers', 'views'
        ]
        choices = sorts + ['-' + k for k in sorts]
        if value not in choices:
            raise ValueError('Temporal coverage does not match the right pattern.')
        return value


class ReuseToIndex(BaseModel):
    id: str
    title: str
    description: str
    url: Optional[str] = None
    badges: Optional[list] = []
    created_at: str
    datasets: int
    followers: int
    views: int
    featured: int
    organization: Optional[dict] = None
    owner: Optional[str] = None
    type: str
    topic: str
    tags: Optional[list] = None
    extras: Optional[dict] = {}


class ReuseArgs(BaseModel):
    q: Optional[str] = None
    page: Optional[int] = 1
    page_size: Optional[int] = 20
    sort: Optional[str] = None
    tag: Optional[str] = None
    badge: Optional[str] = None
    organization: Optional[str] = None
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


@bp.route("/datasets/index", methods=["POST"], endpoint='dataset_index')
@inject
def dataset_index(dataset_service: DatasetService = Provide[Container.dataset_service]):
    try:
        validated_obj = DatasetToIndex(**request.json['data'])
    except ValidationError as e:
        abort(400, e)

    document = DatasetConsumer.load_from_dict(validated_obj.dict())
    dataset_service.feed(document)
    return jsonify({'data': 'Dataset added to index'})


@bp.route("/datasets/<dataset_id>/unindex", methods=["DELETE"], endpoint='dataset_unindex')
@inject
def dataset_unindex(dataset_id: str, dataset_service: DatasetService = Provide[Container.dataset_service]):
    dataset_service.delete_one(dataset_id)
    return jsonify({'data': 'Dataset removed from index'})


@bp.route("/datasets/", methods=["GET"], endpoint='dataset_search')
@inject
def datasets_search(dataset_service: DatasetService = Provide[Container.dataset_service]):
    try:
        request_args = DatasetArgs(**request.args)
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


@bp.route("/organizations/index", methods=["POST"], endpoint='organization_index')
@inject
def organization_index(organization_service: OrganizationService = Provide[Container.organization_service]):
    try:
        validated_obj = OrganizationToIndex(**request.json['data'])
    except ValidationError as e:
        abort(400, e)

    document = OrganizationConsumer.load_from_dict(validated_obj.dict())
    organization_service.feed(document)
    return jsonify({'data': 'Organization added to index'})


@bp.route("/organizations/<organization_id>/unindex", methods=["DELETE"], endpoint='organization_unindex')
@inject
def organization_unindex(organization_id: str, organization_service: OrganizationService = Provide[Container.organization_service]):
    organization_service.delete_one(organization_id)
    return jsonify({'data': 'Organization removed from index'})


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


@bp.route("/reuses/index", methods=["POST"], endpoint='reuse_index')
@inject
def reuse_index(reuse_service: ReuseService = Provide[Container.reuse_service]):
    try:
        validated_obj = ReuseToIndex(**request.json['data'])
    except ValidationError as e:
        abort(400, e)

    document = ReuseConsumer.load_from_dict(validated_obj.dict())
    reuse_service.feed(document)
    return jsonify({'data': 'Reuse added to index'})


@bp.route("/reuses/<reuse_id>/unindex", methods=["DELETE"], endpoint='reuse_unindex')
@inject
def reuse_unindex(reuse_id: str, reuse_service: ReuseService = Provide[Container.reuse_service]):
    reuse_service.delete_one(reuse_id)
    return jsonify({'data': 'Reuse removed from index'})


class RequestReindex(BaseModel):
    key_id: str
    document: Optional[dict] = None
    message_type: str
    index: str


@bp.route("/reindex", methods=["POST"], endpoint='reindex')
@inject
def general_reindex(es: Elasticsearch = Provide[Container.elastic_client]):
    try:
        try:
            validated_obj = RequestReindex(**request.json)
        except ValidationError as e:
            abort(400, e)

        message_type, index_name, data = parse_message(validated_obj.dict())
        index_name = f'{Config.UDATA_INSTANCE_NAME}-{index_name}'
        if message_type == EventMessageType.REINDEX.value:
            # Initiliaze index matching template pattern
            if not es.indices.exists(index=index_name):
                logging.info(f'Initializing new index {index_name} for reindexation')
                es.indices.create(index=index_name)
                # Check whether template with analyzer was correctly assigned
                if 'analysis' not in es.indices.get_settings(index_name)[index_name]['settings']['index']:
                    logging.error(f'Analyzer was not set using templates when initializing {index_name}')
        if message_type in [EventMessageType.INDEX.value,
                            EventMessageType.REINDEX.value]:
            es.index(index=index_name, id=validated_obj.key_id, document=data)
        elif message_type == EventMessageType.UNINDEX.value:
            if es.exists_source(index=index_name, id=validated_obj.key_id):
                es.delete(index=index_name, id=validated_obj.key_id)
    except ValueError:
        abort(400, 'ValueError when parsing message')
    except ConnectionError:
        abort(500, 'ConnectionError with Elastic Client')
    except Exception:
        abort(500, 'Exeption when indexing/unindexing')
    return jsonify({'data': f'Reindex done on {index_name}'})
