from typing import Any, Optional
from fastapi import APIRouter, Request, HTTPException, Depends
from app.infrastructure.services import DatasetService


router = APIRouter()


@router.get('/')
def datasets_search(request: Request, dataset_service: DatasetService = Depends(DatasetService), page: Optional[int] = 1, page_size: Optional[int] = 20, q: Optional[str] = '') -> Any:
    results, results_number, total_pages = dataset_service.search(q, page, page_size)
    next_url = f"{request.url_for('datasets_search')}?page={page + 1}&page_size={page_size}&q={q if q else None}"
    prev_url = f"{request.url_for('datasets_search')}?page={page - 1}&page_size={page_size}&q={q if q else None}"
    return {
        "data": results,
        "next_page": next_url if page < total_pages else None,
        "page": page,
        "previous_page": prev_url if page > 1 else None,
        "page_size": page_size,
        "total_pages": total_pages,
        "total": results_number
    }


@router.get('/{dataset_id}/')
def get_dataset(dataset_id: str, dataset_service: DatasetService = Depends(DatasetService)) -> Any:
    result = dataset_service.find_one(dataset_id)
    if not result:
        raise HTTPException(status_code=404, detail='Dataset not found')
    return result
