from typing import Any, Optional
from fastapi import APIRouter, Request, HTTPException, Depends
from app.infrastructure.services import OrganizationService


router = APIRouter()


@router.get('/')
def organizations_search(request: Request, organization_service: OrganizationService = Depends(OrganizationService), page: int = 1, page_size: int = 20, q: Optional[str] = None) -> Any:
    results, results_number, total_pages = organization_service.search(q, page, page_size)
    next_url = f"{request.url_for('organizations_search')}?page={page + 1}&page_size={page_size}&q={q if q else None}"
    prev_url = f"{request.url_for('organizations_search')}?page={page - 1}&page_size={page_size}&q={q if q else None}"
    return {
        "data": results,
        "next_page": next_url if page < total_pages else None,
        "page": page,
        "previous_page": prev_url if page > 1 else None,
        "page_size": page_size,
        "total_pages": total_pages,
        "total": results_number
    }


@router.get('/{organization_id}/')
def get_organization(organization_id: str, organization_service: OrganizationService = Depends(OrganizationService)) -> Any:
    result = organization_service.find_one(organization_id)
    if not result:
        raise HTTPException(status_code=404, detail='Organization not found')
    return result
