from typing import Any, Optional
from fastapi import APIRouter, Request, HTTPException, Depends
from app.infrastructure.services import ReuseService


router = APIRouter()


@router.get('/')
def reuses_search(request: Request, reuse_service: ReuseService = Depends(ReuseService), page: int = 1, page_size: int = 20, q: Optional[str] = None) -> Any:
    results, results_number, total_pages = reuse_service.search(q, page, page_size)
    next_url = f"{request.url_for('reuses_search')}?page={page + 1}&page_size={page_size}&q={q if q else None}"
    prev_url = f"{request.url_for('reuses_search')}?page={page - 1}&page_size={page_size}&q={q if q else None}"

    return {
        "data": results,
        "next_page": next_url if page < total_pages else None,
        "page": page,
        "previous_page": prev_url if page > 1 else None,
        "page_size": page_size,
        "total_pages": total_pages,
        "total": results_number
    }


@router.get('/{reuse_id}/')
def get_dataset(reuse_id: str, reuse_service: ReuseService = Depends(ReuseService)) -> Any:
    result = reuse_service.find_one(reuse_id)
    if not result:
        raise HTTPException(status_code=404, detail='Dataset not found')
    return result
