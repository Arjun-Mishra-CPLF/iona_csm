"""API routes package."""

from fastapi import APIRouter

from .accounts import router as accounts_router
from .arr import arr_router
from .csm import router as csm_router
from .metrics import router as metrics_router
from .notifications import router as notifications_router
from .preferences import router as preferences_router
from .tasks import router as tasks_router

# Main API router - all routes must be under /api for Databricks Apps OAuth
api_router = APIRouter(prefix="/api")

api_router.include_router(accounts_router, prefix="/accounts", tags=["accounts"])
api_router.include_router(arr_router, prefix="/arr", tags=["arr"])
api_router.include_router(csm_router, prefix="/csm", tags=["csm"])
api_router.include_router(metrics_router, prefix="/metrics", tags=["metrics"])
api_router.include_router(notifications_router, prefix="/notifications", tags=["notifications"])
api_router.include_router(preferences_router, prefix="/preferences", tags=["preferences"])
api_router.include_router(tasks_router, prefix="/tasks", tags=["tasks"])

__all__ = ["api_router"]
