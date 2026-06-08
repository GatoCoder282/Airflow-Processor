from __future__ import annotations

from datetime import date, datetime, timedelta, timezone
from io import BytesIO

from fastapi import APIRouter, Query, Request
from fastapi.responses import StreamingResponse
from openpyxl import Workbook

from ._pagination import clamp_pagination

router = APIRouter(prefix="/urls", tags=["urls"])


@router.get("/by-task")
async def list_urls_by_task(
    request: Request,
    region: str = Query("BO"),
    date_from: date | None = Query(None, description="Fecha inicio (YYYY-MM-DD). Default: últimos 30 días"),
    date_to: date | None = Query(None, description="Fecha fin (YYYY-MM-DD). Default: hoy"),
    limit: int = Query(200),
    offset: int = Query(0),
):
    """
    URLs rotas detectadas via task notify_url_broken=success en task_instance.
    Confirma qué DAG tuvo una URL rota y cuándo.
    """
    if date_from is None:
        date_from = date.today() - timedelta(days=30)
    if date_to is None:
        date_to = date.today()

    limit, offset = clamp_pagination(limit, offset)
    pool = request.app.state.factory.db_pool
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            """
            SELECT
                ti.dag_id,
                dc.criticality,
                dc.source_tag,
                dc.cube_tag,
                COUNT(DISTINCT ti.run_id)       AS detection_count,
                MAX(drm.start_date)::date        AS last_detection_date,
                MIN(drm.start_date)::date        AS first_detection_date
            FROM monitoring.task_instance ti
            JOIN monitoring.dag_run_monitor drm
                ON  drm.dag_id = ti.dag_id
                AND drm.run_id = ti.run_id
                AND drm.region = ti.region
            JOIN monitoring.dag_catalog dc
                ON  dc.dag_id = ti.dag_id
                AND dc.region = ti.region
            WHERE ti.task_id = 'notify_url_broken'
              AND ti.state   = 'success'
              AND ti.region  = $1
              AND ti.start_date::date >= $2
              AND ti.start_date::date <= $3
            GROUP BY ti.dag_id, dc.criticality, dc.source_tag, dc.cube_tag
            ORDER BY
                CASE dc.criticality
                    WHEN 'high'   THEN 1
                    WHEN 'medium' THEN 2
                    WHEN 'low'    THEN 3
                    ELSE 4
                END,
                COUNT(DISTINCT ti.run_id) DESC
            LIMIT $4 OFFSET $5
            """,
            region, date_from, date_to, limit, offset,
        )

    return [dict(r) for r in rows]


@router.get("/by-task/export.xlsx")
async def export_urls_by_task_excel(
    request: Request,
    region: str = Query("BO"),
    date_from: date | None = Query(None),
    date_to: date | None = Query(None),
    limit: int = Query(5000),
):
    """Exporta a Excel las URLs rotas detectadas via task notify_url_broken=success."""
    if date_from is None:
        date_from = date.today() - timedelta(days=30)
    if date_to is None:
        date_to = date.today()

    limit, _ = clamp_pagination(limit, 0)
    pool = request.app.state.factory.db_pool
    async with pool.acquire() as conn:
        rows = [dict(r) for r in await conn.fetch(
            """
            SELECT
                ti.dag_id,
                dc.criticality,
                dc.source_tag,
                dc.cube_tag,
                COUNT(DISTINCT ti.run_id)       AS detection_count,
                MAX(drm.start_date)::date        AS last_detection_date,
                MIN(drm.start_date)::date        AS first_detection_date
            FROM monitoring.task_instance ti
            JOIN monitoring.dag_run_monitor drm
                ON  drm.dag_id = ti.dag_id
                AND drm.run_id = ti.run_id
                AND drm.region = ti.region
            JOIN monitoring.dag_catalog dc
                ON  dc.dag_id = ti.dag_id
                AND dc.region = ti.region
            WHERE ti.task_id = 'notify_url_broken'
              AND ti.state   = 'success'
              AND ti.region  = $1
              AND ti.start_date::date >= $2
              AND ti.start_date::date <= $3
            GROUP BY ti.dag_id, dc.criticality, dc.source_tag, dc.cube_tag
            ORDER BY
                CASE dc.criticality
                    WHEN 'high'   THEN 1
                    WHEN 'medium' THEN 2
                    WHEN 'low'    THEN 3
                    ELSE 4
                END,
                COUNT(DISTINCT ti.run_id) DESC
            LIMIT $4
            """,
            region, date_from, date_to, limit,
        )]

    workbook = Workbook()
    sheet = workbook.active
    sheet.title = "urls_rotas_por_task"

    headers = ["dag_id", "criticality", "source_tag", "cube_tag", "detection_count", "last_detection_date", "first_detection_date"]
    sheet.append(headers)
    for row in rows:
        sheet.append([str(row.get(h)) if row.get(h) is not None else None for h in headers])

    output = BytesIO()
    workbook.save(output)
    output.seek(0)

    timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    filename = f"urls_rotas_task_{timestamp}.xlsx"
    return StreamingResponse(
        output,
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={"Content-Disposition": f'attachment; filename="{filename}"'},
    )
