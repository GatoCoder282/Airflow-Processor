from __future__ import annotations

from fastapi import APIRouter, Query, Request

from ._dag_tags import cubes_subquery
from ._pagination import clamp_pagination

router = APIRouter(prefix="/alerts", tags=["alerts"])


@router.get("")
async def list_alerts(
    request: Request,
    region: str | None = Query(None),
    resolved: bool | None = Query(None),
    severity: str | None = Query(None),
    dag_id: str | None = Query(None),
    alert_type: str | None = Query(None),
    since_days: int | None = Query(None, description="Solo alertas con first_seen_at en los últimos N días"),
    limit: int = Query(100),
    offset: int = Query(0),
):
    """Listado de alertas (solo lectura).

    Las alertas se gestionan automáticamente: se deduplican (occurrence_count) y se
    auto-resuelven cuando el siguiente run del DAG es exitoso. No hay acciones manuales.
    """
    limit, offset = clamp_pagination(limit, offset)
    pool = request.app.state.factory.db_pool
    async with pool.acquire() as conn:
        query = f"""
            SELECT a.id, a.region, a.dag_id, a.run_id, a.task_id,
                   a.severity, a.alert_type, a.incident_category,
                   a.title, a.message, a.exception_snippet,
                   a.occurrence_count, a.suppressed, a.suppression_reason,
                   a.resolved, a.resolved_at, a.auto_resolved, a.resolved_reason,
                   a.resolution_seconds,
                   a.first_seen_at, a.last_seen_at, a.created_at, a.updated_at,
                   dc.source_tag, {cubes_subquery('a')}
            FROM monitoring.alert a
            LEFT JOIN monitoring.dag_catalog dc ON dc.dag_id = a.dag_id AND dc.region = a.region
            WHERE 1=1
        """
        params: list[object] = []
        if region:
            query += f" AND a.region = ${len(params) + 1}"
            params.append(region)
        if resolved is not None:
            query += f" AND a.resolved = ${len(params) + 1}"
            params.append(resolved)
        if severity:
            query += f" AND a.severity = ${len(params) + 1}"
            params.append(severity)
        if dag_id:
            query += f" AND a.dag_id = ${len(params) + 1}"
            params.append(dag_id)
        if alert_type:
            query += f" AND a.alert_type = ${len(params) + 1}"
            params.append(alert_type)
        if since_days is not None:
            query += f" AND a.first_seen_at >= NOW() - (${len(params) + 1} || ' days')::INTERVAL"
            params.append(str(max(int(since_days), 0)))
        query += f" ORDER BY a.created_at DESC LIMIT ${len(params) + 1} OFFSET ${len(params) + 2}"
        params.extend([limit, offset])
        rows = await conn.fetch(query, *params)
        return [dict(row) for row in rows]
