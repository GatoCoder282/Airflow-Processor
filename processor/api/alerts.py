from __future__ import annotations

import asyncio

from fastapi import APIRouter, Body, Query, Request

router = APIRouter(prefix="/alerts", tags=["alerts"])


@router.get("")
async def list_alerts(
    request: Request,
    region: str | None = Query(None),
    resolved: bool | None = Query(None),
    severity: str | None = Query(None),
    dag_id: str | None = Query(None),
    limit: int = Query(100),
    offset: int = Query(0),
):
    pool = request.app.state.factory.db_pool
    async with pool.acquire() as conn:
        query = """
            SELECT id, region, dag_id, run_id, task_id,
                   severity, alert_type, incident_category,
                   title, message, exception_snippet,
                   occurrence_count, suppressed, suppression_reason,
                   resolved, resolved_at, acknowledged, acknowledged_by,
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
        query += f" ORDER BY created_at DESC LIMIT ${len(params) + 1} OFFSET ${len(params) + 2}"
        params.extend([limit, offset])
        rows = await conn.fetch(query, *params)
        return [dict(row) for row in rows]


@router.patch("/bulk-resolve")
async def bulk_resolve_alerts(request: Request, body: dict = Body(...)):
    pool = request.app.state.factory.db_pool
    dag_id = body.get("dag_id")
    alert_ids: list[int] | None = body.get("alert_ids")

    if not dag_id and not alert_ids:
        return {"status": "error", "message": "Se requiere dag_id o alert_ids"}

    async with pool.acquire() as conn:
        if dag_id:
            result = await conn.execute(
                """
                UPDATE monitoring.alert
                SET resolved    = TRUE,
                    resolved_at = NOW(),
                    updated_at  = NOW()
                WHERE dag_id   = $1
                  AND resolved  = FALSE
                  AND suppressed = FALSE
                """,
                dag_id,
            )
        else:
            result = await conn.execute(
                """
                UPDATE monitoring.alert
                SET resolved    = TRUE,
                    resolved_at = NOW(),
                    updated_at  = NOW()
                WHERE id = ANY($1::int[])
                  AND resolved  = FALSE
                """,
                alert_ids,
            )

    count = int(result.split()[-1]) if result else 0
    return {"status": "ok", "resolved_count": count}


@router.patch("/{alert_id}/resolve")
async def resolve_alert(request: Request, alert_id: int):
    pool = request.app.state.factory.db_pool
    notifier = request.app.state.factory.notifier

    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            """
            UPDATE monitoring.alert
            SET resolved = TRUE, resolved_at = NOW(), updated_at = NOW()
            WHERE id = $1
            RETURNING dag_id, title
            """,
            alert_id,
        )

    if row:
        asyncio.create_task(
            notifier.notify_status_change(
                alert_id=alert_id,
                action="resolved",
                dag_id=str(row["dag_id"]),
                title=str(row["title"]),
            )
        )

    return {"status": "ok", "alert_id": alert_id, "resolved": True}


@router.patch("/{alert_id}/acknowledge")
async def acknowledge_alert(request: Request, alert_id: int, body: dict | None = Body(default=None)):
    pool = request.app.state.factory.db_pool
    notifier = request.app.state.factory.notifier
    acknowledged_by = (body or {}).get("acknowledged_by")

    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            """
            UPDATE monitoring.alert
            SET acknowledged = TRUE, acknowledged_by = $2, acknowledged_at = NOW(), updated_at = NOW()
            WHERE id = $1
            RETURNING dag_id, title
            """,
            alert_id,
            acknowledged_by,
        )

    if row:
        asyncio.create_task(
            notifier.notify_status_change(
                alert_id=alert_id,
                action="acknowledged",
                dag_id=str(row["dag_id"]),
                title=str(row["title"]),
                actor=acknowledged_by,
            )
        )

    return {"status": "ok", "alert_id": alert_id, "acknowledged": True}


@router.patch("/{alert_id}/suppress")
async def suppress_alert(request: Request, alert_id: int, body: dict | None = Body(default=None)):
    pool = request.app.state.factory.db_pool
    notifier = request.app.state.factory.notifier
    payload = body or {}
    suppressed = bool(payload.get("suppressed", True))
    suppression_reason = payload.get("suppression_reason")

    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            """
            UPDATE monitoring.alert
            SET suppressed = $2,
                suppression_reason = $3,
                updated_at = NOW()
            WHERE id = $1
            RETURNING dag_id, title
            """,
            alert_id,
            suppressed,
            suppression_reason,
        )

    if row and suppressed:
        asyncio.create_task(
            notifier.notify_status_change(
                alert_id=alert_id,
                action="suppressed",
                dag_id=str(row["dag_id"]),
                title=str(row["title"]),
                suppression_reason=suppression_reason,
            )
        )

    return {
        "status": "ok",
        "alert_id": alert_id,
        "suppressed": suppressed,
        "suppression_reason": suppression_reason,
    }
