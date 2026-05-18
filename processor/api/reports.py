from __future__ import annotations

from datetime import date

from fastapi import APIRouter, Query, Request

router = APIRouter(prefix="/reports", tags=["reports"])


@router.get("")
@router.get("/")
async def list_reports(
    request: Request,
    region: str = Query("BO"),
    semaphore: str | None = Query(None),
    dag_type: str | None = Query(None),
    source_tag: str | None = Query(None),
    date_from: date | None = Query(None, description="Filtrar por last_run_at >= fecha (YYYY-MM-DD)"),
    date_to: date | None = Query(None, description="Filtrar por last_run_at <= fecha (YYYY-MM-DD)"),
    limit: int = Query(200),
    offset: int = Query(0),
):
    """
    Lista la relación código → reporte → estado (DAGs tipo C).
    date_from/date_to filtran por la fecha del último run registrado.
    """
    pool = request.app.state.factory.db_pool
    async with pool.acquire() as conn:
        query = """
            SELECT
                dcs.dag_id,
                dcs.dag_type,
                dcs.dag_subtype,
                dcs.criticality,
                dcs.region,
                dcs.source_tag,
                dcs.cube_tag,
                drl.id_report,
                drl.id_file,
                dcs.file_name,
                drl.expected_publication_window,
                dcs.last_semaphore        AS semaphore,
                dcs.last_semaphore_reason AS semaphore_reason,
                dcs.last_state            AS last_run_state,
                dcs.last_start            AS last_run_at,
                dcs.last_run_id,
                dcs.is_active,
                rre.generated_reports_count,
                rre.expected_reports_count,
                rre.missing_reports_count,
                rre.evaluation_status,
                r.converted_to,
                f.updated_to
            FROM monitoring.dag_report_link drl
            JOIN monitoring.dag_current_status dcs
              ON dcs.dag_id = drl.dag_id AND dcs.region = drl.region
            LEFT JOIN LATERAL (
                SELECT generated_reports_count, expected_reports_count,
                       missing_reports_count, evaluation_status
                FROM monitoring.report_run_expectation
                WHERE dag_id = dcs.dag_id
                  AND region = dcs.region
                ORDER BY created_at DESC
                LIMIT 1
            ) rre ON TRUE
            LEFT JOIN public.report r ON r.id_report = drl.id_report
            LEFT JOIN public.file f   ON f.id_file   = drl.id_file
            WHERE dcs.region = $1
        """
        params: list[object] = [region]

        if semaphore:
            query += f" AND dcs.last_semaphore = ${len(params) + 1}"
            params.append(semaphore)
        if dag_type:
            query += f" AND dcs.dag_type = ${len(params) + 1}"
            params.append(dag_type)
        if source_tag:
            query += f" AND dcs.source_tag ILIKE ${len(params) + 1}"
            params.append(f"%{source_tag}%")
        if date_from:
            query += f" AND dcs.last_start::date >= ${len(params) + 1}"
            params.append(date_from)
        if date_to:
            query += f" AND dcs.last_start::date <= ${len(params) + 1}"
            params.append(date_to)

        query += (
            " ORDER BY CASE dcs.criticality"
            " WHEN 'high' THEN 1 WHEN 'medium' THEN 2 WHEN 'low' THEN 3 ELSE 4 END,"
            " dcs.dag_id"
            f" LIMIT ${len(params) + 1} OFFSET ${len(params) + 2}"
        )
        params.extend([limit, offset])

        rows = await conn.fetch(query, *params)
        return [dict(row) for row in rows]
