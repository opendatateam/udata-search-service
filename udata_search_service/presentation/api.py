import re
from typing import Optional
from dependency_injector.wiring import inject, Provide
from flask import Blueprint, request, url_for, jsonify, abort
from pydantic import BaseModel, Field, ValidationError, validator
from udata_search_service.container import Container
from udata_search_service.infrastructure.services import DatasetService, OrganizationService, ReuseService


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


@bp.route("/datasets/<dataset_id>/", methods=["GET"])
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


@bp.route("/organizations/<organization_id>/", methods=["GET"])
@inject
def get_organization(organization_id: str,
                     organization_service: OrganizationService = Provide[Container.organization_service]):
    result = organization_service.find_one(organization_id)
    if result:
        return jsonify(result)
    abort(404, 'organization not found')


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


@bp.route("/reuses/<reuse_id>/", methods=["GET"])
@inject
def get_reuse(reuse_id: str, reuse_service: ReuseService = Provide[Container.reuse_service]):
    result = reuse_service.find_one(reuse_id)
    if result:
        return jsonify(result)
    abort(404, 'reuse not found')
