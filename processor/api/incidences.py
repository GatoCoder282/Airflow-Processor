from __future__ import annotations

from fastapi import APIRouter, Body, Query, Request

router = APIRouter(prefix="/incidences", tags=["incidences"])


@router.get("")
async def list_incidences(
    request: Request,
    region: str = Query("BO"),
    status: str | None = Query(None),
    category: str | None = Query(None),
    dag_id: str | None = Query(None),
    limit: int = Query(100),
    offset: int = Query(0),
):
    pool = request.app.state.factory.db_pool
    async with pool.acquire() as conn:
        query = "SELECT * FROM monitoring.report_incidence WHERE region = $1"
        params: list[object] = [region]
        if status:
            query += f" AND status = ${len(params) + 1}"
            params.append(status)
        if category:
            query += f" AND category = ${len(params) + 1}"
            params.append(category)
        if dag_id:
            query += f" AND dag_id = ${len(params) + 1}"
            params.append(dag_id)

        query += (
            f" ORDER BY priority_score DESC, detected_at DESC"
            f" LIMIT ${len(params) + 1} OFFSET ${len(params) + 2}"
        )
        params.extend([limit, offset])
        rows = await conn.fetch(query, *params)
        return [dict(row) for row in rows]


@router.patch("/{incidence_id}/status")
async def update_incidence_status(
    request: Request,
    incidence_id: int,
    body: dict = Body(...),
):
    new_status = str(body.get("status", "")).strip()
    observations = body.get("observations")
    allowed = {"open", "in_progress", "resolved", "suppressed"}
    if new_status not in allowed:
        return {"status": "error", "message": "invalid status", "allowed": sorted(allowed)}

    resolved_at_sql = "NOW()" if new_status == "resolved" else "NULL"

    pool = request.app.state.factory.db_pool
    async with pool.acquire() as conn:
        await conn.execute(
            f"""
            UPDATE monitoring.report_incidence
            SET status = $2,
                observations = COALESCE($3, observations),
                resolved_at = {resolved_at_sql},
                updated_at = NOW()
            WHERE id = $1
            """,
            incidence_id,
            new_status,
            observations,
        )

    return {"status": "ok", "incidence_id": incidence_id, "new_status": new_status}


@router.get("/timeline")
async def get_incidences_timeline(
    request: Request,
    region: str = Query("BO"),
    granularity: str = Query("day"),
    category: str | None = Query(None),
    severity: str | None = Query(None),
    limit: int = Query(200),
):
    valid = {"day", "week", "month"}
    if granularity not in valid:
        return {"status": "error", "message": "invalid granularity", "allowed": sorted(valid)}

    period_column = {
        "day": "period_day",
        "week": "period_week",
        "month": "period_month",
    }[granularity]

    pool = request.app.state.factory.db_pool
    async with pool.acquire() as conn:
        query = (
            f"SELECT {period_column} AS period, region, category, severity, "
            f"total_incidences, resolved_count, open_count, avg_resolution_seconds, max_priority_score "
            f"FROM monitoring.incidence_timeline WHERE region = $1"
        )
        params: list[object] = [region]
        if category:
            query += f" AND category = ${len(params) + 1}"
            params.append(category)
        if severity:
            query += f" AND severity = ${len(params) + 1}"
            params.append(severity)
        query += f" ORDER BY period DESC LIMIT ${len(params) + 1}"
        params.append(limit)
        rows = await conn.fetch(query, *params)

    return {
        "granularity": granularity,
        "region": region,
        "rows": [dict(row) for row in rows],
    }
