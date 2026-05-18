from __future__ import annotations

from fastapi import APIRouter, Query, Request

router = APIRouter(prefix="/dags", tags=["dags"])


@router.get("")
@router.get("/")
async def list_dags(
    request: Request,
    region: str = Query("BO"),
    semaphore: str | None = Query(None),
    semaphore_reason: str | None = Query(None),
    criticality: str | None = Query(None),
    dag_type: str | None = Query(None),
    limit: int = Query(100),
    offset: int = Query(0),
):
    pool = request.app.state.factory.db_pool
    async with pool.acquire() as conn:
        query = "SELECT * FROM monitoring.dag_current_status WHERE region = $1 AND is_active = true"
        params: list[object] = [region]
        if semaphore:
            query += f" AND last_semaphore = ${len(params) + 1}"
            params.append(semaphore)
        if semaphore_reason:
            query += f" AND last_semaphore_reason ILIKE ${len(params) + 1}"
            params.append(f"%{semaphore_reason}%")
        if criticality:
            query += f" AND criticality = ${len(params) + 1}"
            params.append(criticality)
        if dag_type:
            query += f" AND dag_type = ${len(params) + 1}"
            params.append(dag_type)
        query += (
            " ORDER BY CASE criticality"
            " WHEN 'high' THEN 1 WHEN 'medium' THEN 2 WHEN 'low' THEN 3 ELSE 4 END,"
            " dag_id"
            f" LIMIT ${len(params) + 1} OFFSET ${len(params) + 2}"
        )
        params.extend([limit, offset])
        rows = await conn.fetch(query, *params)
        return [dict(row) for row in rows]


@router.get("/failed-history")
async def get_failed_dag_history(
    request: Request,
    region: str = Query("BO"),
    days: int = Query(30),
    dag_id: str | None = Query(None),
    dag_type: str | None = Query(None),
    status: str | None = Query(None, description="'resolved' o 'still_failing'"),
    limit: int = Query(200),
    offset: int = Query(0),
):
    pool = request.app.state.factory.db_pool
    async with pool.acquire() as conn:
        conditions = ["region = $1", "failed_at >= NOW() - ($2 || ' days')::INTERVAL"]
        params: list[object] = [region, str(days)]
        if dag_id:
            params.append(dag_id)
            conditions.append(f"dag_id = ${len(params)}")
        if dag_type:
            params.append(dag_type)
            conditions.append(
                f"dag_id IN (SELECT dag_id FROM monitoring.dag_catalog"
                f" WHERE region = $1 AND dag_type = ${len(params)})"
            )
        if status:
            params.append(status)
            conditions.append(f"failure_status = ${len(params)}")
        where = " AND ".join(conditions)
        params.extend([limit, offset])
        rows = await conn.fetch(
            f"SELECT * FROM monitoring.dag_failure_history WHERE {where}"
            f" ORDER BY failed_at DESC LIMIT ${len(params) - 1} OFFSET ${len(params)}",
            *params,
        )
        return [dict(r) for r in rows]


@router.get("/{dag_id}")
async def get_dag(request: Request, dag_id: str, region: str = Query("BO")):
    pool = request.app.state.factory.db_pool
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            """
            SELECT *
            FROM monitoring.dag_run_monitor
            WHERE dag_id = $1 AND region = $2
            ORDER BY updated_at DESC
            LIMIT 100
            """,
            dag_id,
            region,
        )
        return [dict(row) for row in rows]


@router.get("/{dag_id}/runs/{run_id}/root-cause")
async def get_run_root_cause(request: Request, dag_id: str, run_id: str, region: str = Query("BO")):
    pool = request.app.state.factory.db_pool
    async with pool.acquire() as conn:
        root_row = await conn.fetchrow(
            """
            SELECT task_id, state, upstream_task_id, log_excerpt, last_log_token, updated_at
            FROM monitoring.task_instance
            WHERE dag_id = $1 AND run_id = $2 AND region = $3
              AND state IN ('failed', 'upstream_failed')
            ORDER BY CASE WHEN state = 'failed' THEN 0 ELSE 1 END,
                     updated_at DESC
            LIMIT 1
            """,
            dag_id,
            run_id,
            region,
        )

        tasks = await conn.fetch(
            """
            SELECT task_id, state, upstream_task_id, downstream_task_ids,
                   log_excerpt, last_log_token, updated_at
            FROM monitoring.task_instance
            WHERE dag_id = $1 AND run_id = $2 AND region = $3
            ORDER BY updated_at DESC
            """,
            dag_id,
            run_id,
            region,
        )

        report_row = await conn.fetchrow(
            """
            SELECT id_report, id_file
            FROM monitoring.dag_report_link
            WHERE dag_id = $1 AND region = $2
            ORDER BY is_primary DESC, created_at DESC
            LIMIT 1
            """,
            dag_id,
            region,
        )

    root_cause = dict(root_row) if root_row else None
    report = dict(report_row) if report_row else None
    return {
        "dag_id": dag_id,
        "run_id": run_id,
        "region": region,
        "root_cause": root_cause,
        "affected_report": report,
        "tasks": [dict(row) for row in tasks],
    }