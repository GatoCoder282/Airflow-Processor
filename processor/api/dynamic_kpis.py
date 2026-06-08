from __future__ import annotations

from datetime import date, timedelta

from fastapi import APIRouter, Query, Request

router = APIRouter(prefix="/kpis/dynamic", tags=["kpis-dynamic"])


def _default_from() -> date:
    return date.today() - timedelta(days=6)


def _default_to() -> date:
    return date.today()


@router.get("")
async def get_dynamic_kpis(
    request: Request,
    region: str = Query("BO"),
    date_from: date = Query(default=None, description="Fecha inicio (YYYY-MM-DD). Default: hace 7 días"),
    date_to: date = Query(default=None, description="Fecha fin (YYYY-MM-DD). Default: hoy"),
):
    """KPIs con rango de fechas libre.

    Reemplaza /kpis/extended cuando se necesita filtrar por fechas específicas.
    Todos los conteos respetan date_from <= fecha <= date_to.
    """
    if date_from is None:
        date_from = _default_from()
    if date_to is None:
        date_to = _default_to()

    pool = request.app.state.factory.db_pool
    async with pool.acquire() as conn:

        # ── Runs en el rango ──────────────────────────────────────────────────
        runs_row = await conn.fetchrow(
            """
            SELECT
                COUNT(*)                              AS total_runs,
                COUNT(*) FILTER (WHERE state = 'failed') AS failed_runs,
                ROUND(
                    COUNT(*) FILTER (WHERE state = 'failed') * 100.0
                    / NULLIF(COUNT(*), 0)
                , 1)                                  AS failure_rate_pct
            FROM monitoring.dag_run_monitor
            WHERE region = $1
              AND start_date::date >= $2
              AND start_date::date <= $3
            """,
            region, date_from, date_to,
        )

        # ── Alertas abiertas al final del rango ───────────────────────────────
        alerts_row = await conn.fetchrow(
            """
            SELECT
                COUNT(*)                                    AS open_alerts,
                COUNT(*) FILTER (WHERE severity = 'critical') AS open_critical
            FROM monitoring.alert
            WHERE region = $1
              AND resolved = FALSE
              AND created_at::date <= $2
            """,
            region, date_to,
        )

        # ── Alertas creadas en el rango ───────────────────────────────────────
        alerts_created = await conn.fetchval(
            """
            SELECT COUNT(*)
            FROM monitoring.alert
            WHERE region = $1
              AND created_at::date >= $2
              AND created_at::date <= $3
            """,
            region, date_from, date_to,
        )

        # ── URLs muertas en el rango ──────────────────────────────────────────
        dead_urls = await conn.fetchval(
            """
            SELECT COUNT(*)
            FROM monitoring.broken_url_summary
            WHERE execution_date::date >= $1
              AND execution_date::date <= $2
            """,
            date_from, date_to,
        )

    runs = dict(runs_row) if runs_row else {}
    alerts = dict(alerts_row) if alerts_row else {}

    return {
        "region": region,
        "period": {
            "date_from": date_from.isoformat(),
            "date_to": date_to.isoformat(),
            "days": (date_to - date_from).days + 1,
        },
        "runs": {
            "total": int(runs.get("total_runs") or 0),
            "failed": int(runs.get("failed_runs") or 0),
            "failure_rate_pct": float(runs.get("failure_rate_pct") or 0.0),
        },
        "alerts": {
            "open": int(alerts.get("open_alerts") or 0),
            "open_critical": int(alerts.get("open_critical") or 0),
            "created_in_period": int(alerts_created or 0),
        },
        "urls": {
            "dead_urls_in_period": int(dead_urls or 0),
        },
    }


@router.get("/runs-by-day")
async def get_runs_by_day(
    request: Request,
    region: str = Query("BO"),
    date_from: date = Query(default=None, description="Fecha inicio (YYYY-MM-DD). Default: hace 7 días"),
    date_to: date = Query(default=None, description="Fecha fin (YYYY-MM-DD). Default: hoy"),
):
    """Desglose diario de runs (total, fallidos, tasa) para graficar en el dashboard."""
    if date_from is None:
        date_from = _default_from()
    if date_to is None:
        date_to = _default_to()

    pool = request.app.state.factory.db_pool
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            """
            SELECT
                start_date::date                              AS day,
                COUNT(*)                                      AS total_runs,
                COUNT(*) FILTER (WHERE state = 'failed')      AS failed_runs,
                ROUND(
                    COUNT(*) FILTER (WHERE state = 'failed') * 100.0
                    / NULLIF(COUNT(*), 0)
                , 1)                                          AS failure_rate_pct
            FROM monitoring.dag_run_monitor
            WHERE region = $1
              AND start_date::date >= $2
              AND start_date::date <= $3
            GROUP BY start_date::date
            ORDER BY day
            """,
            region, date_from, date_to,
        )
    return [dict(row) for row in rows]


@router.get("/alerts-by-day")
async def get_alerts_by_day(
    request: Request,
    region: str = Query("BO"),
    date_from: date = Query(default=None, description="Fecha inicio (YYYY-MM-DD). Default: hace 7 días"),
    date_to: date = Query(default=None, description="Fecha fin (YYYY-MM-DD). Default: hoy"),
):
    """Desglose diario de alertas creadas (por severidad) para graficar."""
    if date_from is None:
        date_from = _default_from()
    if date_to is None:
        date_to = _default_to()

    pool = request.app.state.factory.db_pool
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            """
            SELECT
                created_at::date                                    AS day,
                COUNT(*)                                            AS total_alerts,
                COUNT(*) FILTER (WHERE severity = 'critical')       AS critical,
                COUNT(*) FILTER (WHERE severity = 'warning')        AS warning,
                COUNT(*) FILTER (WHERE severity = 'info')           AS info
            FROM monitoring.alert
            WHERE region = $1
              AND created_at::date >= $2
              AND created_at::date <= $3
            GROUP BY created_at::date
            ORDER BY day
            """,
            region, date_from, date_to,
        )
    return [dict(row) for row in rows]
