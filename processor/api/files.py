from __future__ import annotations

from datetime import date

from fastapi import APIRouter, Query, Request

from ._dag_tags import cubes_subquery
from ._pagination import clamp_pagination

router = APIRouter(prefix="/files", tags=["files"])


@router.get("")
@router.get("/")
async def list_files(
    request: Request,
    region: str = Query("BO"),
    source_tag: str | None = Query(None),
    semaphore: str | None = Query(None),
    date_from: date | None = Query(None, description="Filtrar por last_start >= fecha (YYYY-MM-DD)"),
    date_to: date | None = Query(None, description="Filtrar por last_start <= fecha (YYYY-MM-DD)"),
    limit: int = Query(200),
    offset: int = Query(0),
):
    """
    Lista el estado de descarga de DAGs tipo D.
    El outcome se determina por la task de notificación con state=success en el último run.
    """
    limit, offset = clamp_pagination(limit, offset)
    cubes = cubes_subquery("dc")
    pool = request.app.state.factory.db_pool
    async with pool.acquire() as conn:
        query = f"""
            SELECT
                dc.dag_id,
                dc.region,
                dc.source_tag,
                {cubes},
                dc.criticality,
                dc.dag_type,
                dcs.last_semaphore,
                dcs.last_semaphore_reason,
                dcs.last_state,
                dcs.last_start,
                dcs.last_run_id,
                dcs.is_active,
                CASE
                    WHEN ti_upd.state  = 'success' THEN 'download'
                    WHEN ti_rev.state  = 'success' THEN 'revision_file'
                    WHEN ti_drev.state = 'success' THEN 'revision_data'
                    WHEN ti_url.state  = 'success' THEN 'url_broken'
                    ELSE 'unknown'
                END AS last_run_outcome
            FROM monitoring.dag_catalog dc
            JOIN monitoring.dag_current_status dcs
              ON dcs.dag_id = dc.dag_id AND dcs.region = dc.region
            LEFT JOIN LATERAL (
                SELECT state FROM monitoring.task_instance
                WHERE dag_id  = dc.dag_id AND region = dc.region
                  AND run_id  = dcs.last_run_id AND task_id = 'update_file'
                ORDER BY try_number DESC LIMIT 1
            ) ti_upd ON TRUE
            LEFT JOIN LATERAL (
                SELECT state FROM monitoring.task_instance
                WHERE dag_id  = dc.dag_id AND region = dc.region
                  AND run_id  = dcs.last_run_id AND task_id = 'notify_success_revision_only'
                ORDER BY try_number DESC LIMIT 1
            ) ti_rev ON TRUE
            LEFT JOIN LATERAL (
                SELECT state FROM monitoring.task_instance
                WHERE dag_id  = dc.dag_id AND region = dc.region
                  AND run_id  = dcs.last_run_id AND task_id = 'notify_success_download_revision'
                ORDER BY try_number DESC LIMIT 1
            ) ti_drev ON TRUE
            LEFT JOIN LATERAL (
                SELECT state FROM monitoring.task_instance
                WHERE dag_id  = dc.dag_id AND region = dc.region
                  AND run_id  = dcs.last_run_id AND task_id = 'notify_url_broken'
                ORDER BY try_number DESC LIMIT 1
            ) ti_url ON TRUE
            WHERE dc.region = $1
              AND dc.dag_type = 'D'
              AND dcs.is_active = true
        """
        params: list[object] = [region]

        if source_tag:
            query += f" AND dc.source_tag ILIKE ${len(params) + 1}"
            params.append(f"%{source_tag}%")
        if semaphore:
            query += f" AND dcs.last_semaphore = ${len(params) + 1}"
            params.append(semaphore)
        if date_from:
            query += f" AND dcs.last_start::date >= ${len(params) + 1}"
            params.append(date_from)
        if date_to:
            query += f" AND dcs.last_start::date <= ${len(params) + 1}"
            params.append(date_to)

        query += (
            " ORDER BY CASE dc.criticality"
            " WHEN 'high' THEN 1 WHEN 'medium' THEN 2 WHEN 'low' THEN 3 ELSE 4 END,"
            " dc.dag_id"
            f" LIMIT ${len(params) + 1} OFFSET ${len(params) + 2}"
        )
        params.extend([limit, offset])

        rows = await conn.fetch(query, *params)
        return [dict(row) for row in rows]
