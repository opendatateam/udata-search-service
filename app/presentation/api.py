from dependency_injector.wiring import inject, Provide
from flask import Blueprint, request, url_for, jsonify, abort
from app.container import Container
from app.infrastructure.services import DatasetService, OrganizationService, ReuseService


bp = Blueprint('api', __name__, url_prefix='/api/1')


@bp.route("/organizations/", methods=["GET"], endpoint='organization_search')
@inject
def organizations_search(organization_service: OrganizationService = Provide[Container.organization_service]):
    page = request.args.get('page', 1, type=int)
    page_size = request.args.get('page_size', 20, type=int)
    query_text = request.args.get('q', None)

    if not query_text:
        abort(400)

    results, results_number, total_pages = organization_service.search(query_text, page, page_size)
    next_url = url_for('api.organization_search', q=query_text, page=page + 1, page_size=page_size, _external=True)
    prev_url = url_for('api.organization_search', q=query_text, page=page - 1, page_size=page_size, _external=True)

    return jsonify({
        "data": results,
        "next_page": next_url if page < total_pages else None,
        "page": page,
        "previous_page": prev_url if page > 1 else None,
        "page_size": page_size,
        "total_pages": total_pages,
        "total": results_number
    })


@bp.route("/organizations/<organization_id>/", methods=["GET"])
@inject
def get_organization(organization_id: str, organization_service: DatasetService = Provide[Container.organization_service]):
    result = organization_service.find_one(organization_id)
    if result:
        return jsonify(result)
    abort(404, 'organization not found')


@bp.route("/datasets/", methods=["GET"], endpoint='dataset_search')
@inject
def datasets_search(dataset_service: DatasetService = Provide[Container.dataset_service]):
    page = request.args.get('page', 1, type=int)
    page_size = request.args.get('page_size', 20, type=int)
    query_text = request.args.get('q', None)

    if not query_text:
        abort(400)

    results, results_number, total_pages = dataset_service.search(query_text, page, page_size)
    next_url = url_for('api.dataset_search', q=query_text, page=page + 1, page_size=page_size, _external=True)
    prev_url = url_for('api.dataset_search', q=query_text, page=page - 1, page_size=page_size, _external=True)

    return jsonify({
        "data": results,
        "next_page": next_url if page < total_pages else None,
        "page": page,
        "previous_page": prev_url if page > 1 else None,
        "page_size": page_size,
        "total_pages": total_pages,
        "total": results_number
    })


@bp.route("/datasets/<dataset_id>/", methods=["GET"])
@inject
def get_dataset(dataset_id: str, dataset_service: DatasetService = Provide[Container.dataset_service]):
    result = dataset_service.find_one(dataset_id)
    if result:
        return jsonify(result)
    abort(404, 'dataset not found')


@bp.route("/reuses/", methods=["GET"], endpoint='reuse_search')
@inject
def reuses_search(reuse_service: ReuseService = Provide[Container.reuse_service]):
    page = request.args.get('page', 1, type=int)
    page_size = request.args.get('page_size', 20, type=int)
    query_text = request.args.get('q', None)

    if not query_text:
        abort(400)

    results, results_number, total_pages = reuse_service.search(query_text, page, page_size)
    next_url = url_for('api.reuse_search', q=query_text, page=page + 1, page_size=page_size, _external=True)
    prev_url = url_for('api.reuse_search', q=query_text, page=page - 1, page_size=page_size, _external=True)

    return jsonify({
        "data": results,
        "next_page": next_url if page < total_pages else None,
        "page": page,
        "previous_page": prev_url if page > 1 else None,
        "page_size": page_size,
        "total_pages": total_pages,
        "total": results_number
    })


@bp.route("/reuses/<reuse_id>/", methods=["GET"])
@inject
def get_reuse(reuse_id: str, reuse_service: ReuseService = Provide[Container.reuse_service]):
    result = reuse_service.find_one(reuse_id)
    if result:
        return jsonify(result)
    abort(404, 'reuse not found')
