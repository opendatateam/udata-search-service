from fastapi import APIRouter

from app.presentation.api.api_v1.endpoints import dataset, organization, reuse

api_router = APIRouter()
api_router.include_router(dataset.router, prefix='/datasets', tags=['dataset'])
api_router.include_router(organization.router, prefix='/organizations', tags=['organization'])
api_router.include_router(reuse.router, prefix='/reuses', tags=['reuse'])
