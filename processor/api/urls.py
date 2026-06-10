from __future__ import annotations

from datetime import date, datetime, timedelta, timezone
from io import BytesIO

from fastapi import APIRouter, Query, Request
from fastapi.responses import StreamingResponse
from openpyxl import Workbook

from ._dag_tags import cubes_subquery
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
    cubes = cubes_subquery("ti")
    pool = request.app.state.factory.db_pool
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            f"""
            SELECT
                ti.dag_id,
                dc.criticality,
                dc.source_tag,
                {cubes},
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
            GROUP BY ti.dag_id, ti.region, dc.criticality, dc.source_tag
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

    result = [dict(r) for r in rows]

    # Enriquecimiento aditivo desde platform_db (tolerante: {} si no disponible).
    dag_ids = [r["dag_id"] for r in result if r.get("dag_id")]
    enrichment = await request.app.state.factory.platform.broken_urls_for(dag_ids)
    for row in result:
        info = enrichment.get(row["dag_id"], {})
        row["file_name"] = info.get("file_name")
        row["path"] = info.get("path")
        row["datos_a"] = info.get("datos_a")
        row["main_url"] = info.get("main_url")
        row["broken_url"] = info.get("broken_url")
    return result


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
    cubes = cubes_subquery("ti")
    pool = request.app.state.factory.db_pool
    async with pool.acquire() as conn:
        rows = [dict(r) for r in await conn.fetch(
            f"""
            SELECT
                ti.dag_id,
                dc.criticality,
                dc.source_tag,
                {cubes},
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
            GROUP BY ti.dag_id, ti.region, dc.criticality, dc.source_tag
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

    headers = ["dag_id", "criticality", "source_tag", "cubes", "detection_count", "last_detection_date", "first_detection_date"]
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
