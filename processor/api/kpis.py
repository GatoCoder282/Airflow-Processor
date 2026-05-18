from __future__ import annotations

from datetime import date
from typing import Optional

from fastapi import APIRouter, Query, Request

router = APIRouter(tags=["kpis"])


@router.get("/kpis/extended")
async def get_kpis_extended(request: Request, region: str = Query("BO")):
    pool = request.app.state.factory.db_pool
    async with pool.acquire() as conn:
        base = await conn.fetchrow(
            """
            SELECT *
            FROM monitoring.kpi_summary
            WHERE region = $1
            LIMIT 1
            """,
            region,
        )

        missing_reports_today = await conn.fetchval(
            """
            SELECT COUNT(*)
            FROM monitoring.report_run_expectation
            WHERE region = $1
              AND created_at >= CURRENT_DATE
              AND missing_reports_count > 0
            """,
            region,
        )
        total_expectations_today = await conn.fetchval(
            """
            SELECT COUNT(*)
            FROM monitoring.report_run_expectation
            WHERE region = $1
              AND created_at >= CURRENT_DATE
            """,
            region,
        )

        total_runs_week = await conn.fetchval(
            """
            SELECT COUNT(*)
            FROM monitoring.dag_run_monitor
            WHERE region = $1
              AND start_date >= CURRENT_DATE - INTERVAL '7 days'
            """,
            region,
        )

        dead_urls_30d = await conn.fetchval(
            """
            SELECT COUNT(*)
            FROM monitoring.broken_url_summary
            WHERE execution_date >= NOW() - INTERVAL '30 days'
            """
        )

    base_dict = dict(base) if base else {}
    missing_reports_today = int(missing_reports_today or 0)
    total_expectations_today = int(total_expectations_today or 0)
    total_runs_week = int(total_runs_week or 0)
    dead_urls_30d = int(dead_urls_30d or 0)

    missing_reports_index = round(
        (missing_reports_today * 100.0 / total_expectations_today) if total_expectations_today else 0.0,
        2,
    )

    return {
        "region": region,
        "daily": {
            "failed_runs": int(base_dict.get("failed_runs_today", 0) or 0),
            "total_runs": int(base_dict.get("total_runs_today", 0) or 0),
            "failure_rate_pct": float(base_dict.get("failure_rate_today_pct", 0.0) or 0.0),
        },
        "weekly": {
            "failed_runs": int(base_dict.get("failed_runs_week", 0) or 0),
            "total_runs": total_runs_week,
            "failure_rate_pct": float(base_dict.get("failure_rate_week_pct", 0.0) or 0.0),
        },
        "alerts": {
            "open": int(base_dict.get("open_alerts", 0) or 0),
            "open_critical": int(base_dict.get("open_critical_alerts", 0) or 0),
        },
        "reporting": {
            "runs_with_no_reports_today": int(base_dict.get("runs_with_no_reports_today", 0) or 0),
            "missing_reports_today": missing_reports_today,
            "missing_reports_count": missing_reports_today,
            "total_reports_expected": total_expectations_today,
            "missing_reports_index_pct": missing_reports_index,
        },
        "urls": {
            "dead_urls_30d": dead_urls_30d,
        },
    }


@router.get("/kpis/today")
async def get_kpis_today(
    request: Request,
    region: str = Query("BO"),
    execution_date: Optional[date] = Query(None, description="Fecha a consultar (YYYY-MM-DD). Default: hoy"),
):
    """Resumen completo de ejecuciones del día.

    Incluye: totales, descargas exitosas, revisiones, URLs rotas,
    tipo de run (manual/scheduled) y top 15 de mayor duración.
    Por defecto consulta el día actual; usar execution_date para fechas anteriores.
    """
    target_date = execution_date or date.today()
    pool = request.app.state.factory.db_pool
    async with pool.acquire() as conn:

        # ── Resumen de ejecuciones + tipo de run ──────────────────────────────
        exec_row = await conn.fetchrow(
            """
            SELECT
                COUNT(*)                                                    AS total,
                COUNT(*) FILTER (WHERE state = 'success')                   AS success_count,
                COUNT(*) FILTER (WHERE state = 'failed')                    AS failed_count,
                COUNT(*) FILTER (WHERE state NOT IN ('success', 'failed'))  AS running_count,
                ROUND(
                    COUNT(*) FILTER (WHERE state = 'failed') * 100.0
                    / NULLIF(COUNT(*), 0)
                , 1)                                                        AS failure_rate_pct,
                COUNT(*) FILTER (WHERE run_type = 'manual')                 AS manual_count,
                COUNT(*) FILTER (WHERE run_type = 'scheduled')              AS scheduled_count
            FROM monitoring.dag_run_monitor
            WHERE region = $1
              AND start_date::date = $2
            """,
            region, target_date,
        )

        # ── Descargas exitosas: task update_file=success ─────────────────────────
        _order_by_criticality = """
            ORDER BY
                CASE dc.criticality
                    WHEN 'high'   THEN 1
                    WHEN 'medium' THEN 2
                    WHEN 'low'    THEN 3
                    ELSE 4
                END,
                drm.dag_id
        """
        _base_from = """
            FROM monitoring.dag_run_monitor drm
            JOIN monitoring.dag_catalog dc
                ON  dc.dag_id = drm.dag_id
                AND dc.region = drm.region
            WHERE drm.region = $1
              AND drm.start_date::date = $2
              AND dc.dag_type = 'D'
        """
        download_rows = await conn.fetch(
            f"""
            SELECT drm.dag_id, drm.run_id, drm.duration_seconds, dc.criticality
            {_base_from}
              AND EXISTS (
                  SELECT 1 FROM monitoring.task_instance ti
                  WHERE ti.dag_id  = drm.dag_id
                    AND ti.run_id  = drm.run_id
                    AND ti.region  = drm.region
                    AND ti.task_id = 'update_file'
                    AND ti.state   = 'success'
              )
            {_order_by_criticality}
            """,
            region, target_date,
        )

        # ── Revisiones de archivo: task notify_success_revision_only=success ──
        revision_rows = await conn.fetch(
            f"""
            SELECT drm.dag_id, drm.run_id, drm.duration_seconds, dc.criticality
            {_base_from}
              AND EXISTS (
                  SELECT 1 FROM monitoring.task_instance ti
                  WHERE ti.dag_id  = drm.dag_id
                    AND ti.run_id  = drm.run_id
                    AND ti.region  = drm.region
                    AND ti.task_id = 'notify_success_revision_only'
                    AND ti.state   = 'success'
              )
            {_order_by_criticality}
            """,
            region, target_date,
        )

        # ── Revisiones de contenido: task notify_success_download_revision=success ──
        download_revision_rows = await conn.fetch(
            f"""
            SELECT drm.dag_id, drm.run_id, drm.duration_seconds, dc.criticality
            {_base_from}
              AND EXISTS (
                  SELECT 1 FROM monitoring.task_instance ti
                  WHERE ti.dag_id  = drm.dag_id
                    AND ti.run_id  = drm.run_id
                    AND ti.region  = drm.region
                    AND ti.task_id = 'notify_success_download_revision'
                    AND ti.state   = 'success'
              )
            {_order_by_criticality}
            """,
            region, target_date,
        )

        # ── URLs rotas del día: task notify_url_broken=success ────────────────
        broken_rows = await conn.fetch(
            """
            SELECT dag_id, run_id, criticality
            FROM (
                SELECT DISTINCT
                    drm.dag_id,
                    drm.run_id,
                    dc.criticality
                FROM monitoring.dag_run_monitor drm
                JOIN monitoring.dag_catalog dc
                    ON  dc.dag_id  = drm.dag_id
                    AND dc.region  = drm.region
                WHERE drm.region = $1
                  AND drm.start_date::date = $2
                  AND EXISTS (
                      SELECT 1 FROM monitoring.task_instance ti
                      WHERE ti.dag_id  = drm.dag_id
                        AND ti.run_id  = drm.run_id
                        AND ti.region  = drm.region
                        AND ti.task_id = 'notify_url_broken'
                        AND ti.state   = 'success'
                  )
            ) sub
            ORDER BY
                CASE criticality
                    WHEN 'high'   THEN 1
                    WHEN 'medium' THEN 2
                    WHEN 'low'    THEN 3
                    ELSE 4
                END,
                dag_id
            """,
            region, target_date,
        )

        # ── Alertas críticas abiertas ─────────────────────────────────────────
        open_critical = await conn.fetchval(
            """
            SELECT COUNT(*)
            FROM monitoring.alert
            WHERE region   = $1
              AND severity  = 'critical'
              AND resolved  = FALSE
            """,
            region,
        )

        # ── Top 15 de mayor duración ──────────────────────────────────────────
        top15_rows = await conn.fetch(
            """
            SELECT
                dag_id,
                run_id,
                run_type,
                state,
                start_date,
                end_date,
                duration_seconds
            FROM monitoring.dag_run_monitor
            WHERE region = $1
              AND start_date::date  = $2
              AND duration_seconds IS NOT NULL
            ORDER BY duration_seconds DESC
            LIMIT 15
            """,
            region, target_date,
        )

    exec_d = dict(exec_row) if exec_row else {}

    return {
        "region": region,
        "date": str(target_date),
        "executions": {
            "total": int(exec_d.get("total") or 0),
            "success": int(exec_d.get("success_count") or 0),
            "failed": int(exec_d.get("failed_count") or 0),
            "running": int(exec_d.get("running_count") or 0),
            "failure_rate_pct": float(exec_d.get("failure_rate_pct") or 0.0),
            "manual_count": int(exec_d.get("manual_count") or 0),
            "scheduled_count": int(exec_d.get("scheduled_count") or 0),
        },
        "downloads": {
            "count": len(download_rows),
            "summary": [dict(r) for r in download_rows],
        },
        "revisions": {
            "count": len(revision_rows),
            "summary": [dict(r) for r in revision_rows],
        },
        "download_revisions": {
            "count": len(download_revision_rows),
            "summary": [dict(r) for r in download_revision_rows],
        },
        "broken_urls": {
            "count": len(broken_rows),
            "summary": [dict(r) for r in broken_rows],
        },
        "open_critical_alerts": int(open_critical or 0),
        "top15_longest": [dict(r) for r in top15_rows],
    }
