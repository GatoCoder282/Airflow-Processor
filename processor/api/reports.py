from __future__ import annotations

from fastapi import APIRouter, Request

router = APIRouter(prefix="/reports", tags=["reports"])


@router.get("/by-dag/{dag_id}")
async def get_reports_by_dag(request: Request, dag_id: str):
    """Reportes generados por el archivo de un DAG y los cubos/bases que alimentan.

    Solo lectura desde platform_db (no toca monitoring). Devuelve ``[]`` si platform_db
    no está disponible o el DAG no tiene reportes asociados — nunca un error.
    """
    return await request.app.state.factory.platform.reports_by_dag(dag_id)
