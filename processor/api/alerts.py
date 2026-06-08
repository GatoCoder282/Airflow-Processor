from __future__ import annotations

from fastapi import APIRouter, Query, Request

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
        query = """
            SELECT id, region, dag_id, run_id, task_id,
                   severity, alert_type, incident_category,
                   title, message, exception_snippet,
                   occurrence_count, suppressed, suppression_reason,
                   resolved, resolved_at, auto_resolved, resolved_reason,
                   resolution_seconds,
                   first_seen_at, last_seen_at, created_at, updated_at
            FROM monitoring.alert
            WHERE 1=1
        """
        params: list[object] = []
        if region:
            query += f" AND region = ${len(params) + 1}"
            params.append(region)
        if resolved is not None:
            query += f" AND resolved = ${len(params) + 1}"
            params.append(resolved)
        if severity:
            query += f" AND severity = ${len(params) + 1}"
            params.append(severity)
        if dag_id:
            query += f" AND dag_id = ${len(params) + 1}"
            params.append(dag_id)
        if alert_type:
            query += f" AND alert_type = ${len(params) + 1}"
            params.append(alert_type)
        if since_days is not None:
            query += f" AND first_seen_at >= NOW() - (${len(params) + 1} || ' days')::INTERVAL"
            params.append(str(max(int(since_days), 0)))
        query += f" ORDER BY created_at DESC LIMIT ${len(params) + 1} OFFSET ${len(params) + 2}"
        params.extend([limit, offset])
        rows = await conn.fetch(query, *params)
        return [dict(row) for row in rows]
