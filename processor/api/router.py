from __future__ import annotations

from fastapi import APIRouter

from .alerts import router as alerts_router
from .dags import router as dags_router
from .dynamic_kpis import router as dynamic_kpis_router
from .files import router as files_router
from .health import router as health_router
from .incidences import router as incidences_router
from .kpis import router as kpis_router
from .reports import router as reports_router
from .urls import router as urls_router

router = APIRouter()
router.include_router(health_router)
router.include_router(dags_router)
router.include_router(incidences_router)
router.include_router(files_router)
router.include_router(urls_router)
router.include_router(alerts_router)
router.include_router(kpis_router)
router.include_router(dynamic_kpis_router)
router.include_router(reports_router)