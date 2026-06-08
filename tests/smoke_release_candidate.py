from __future__ import annotations

import asyncio
import os

import asyncpg
import httpx
from dotenv import load_dotenv


async def main() -> int:
    load_dotenv()
    dsn = os.getenv("DATABASE_URL")
    if not dsn:
        print("ERROR: DATABASE_URL missing")
        return 1

    pool = await asyncpg.create_pool(dsn=dsn, min_size=1, max_size=2)
    try:
        async with pool.acquire() as conn:
            await conn.execute("REFRESH MATERIALIZED VIEW CONCURRENTLY monitoring.dag_current_status")
            await conn.execute("REFRESH MATERIALIZED VIEW CONCURRENTLY monitoring.dag_performance_stats")
            await conn.execute("REFRESH MATERIALIZED VIEW CONCURRENTLY monitoring.broken_url_summary")
            await conn.execute("REFRESH MATERIALIZED VIEW CONCURRENTLY monitoring.broken_url_priority")

            await conn.fetch("SELECT * FROM monitoring.kpi_summary LIMIT 5")
            await conn.fetch("SELECT * FROM monitoring.broken_url_priority LIMIT 5")

        api_base = os.getenv("API_BASE_URL", "http://localhost:8000")
        async with httpx.AsyncClient(timeout=10) as client:
            health = await client.get(f"{api_base}/health")
            ready = await client.get(f"{api_base}/ready")
            kpis = await client.get(f"{api_base}/kpis/extended", params={"region": "BO"})

        if health.status_code != 200 or ready.status_code not in {200, 503} or kpis.status_code != 200:
            print("ERROR: API smoke endpoints failed", health.status_code, ready.status_code, kpis.status_code)
            return 1

        print("SMOKE RELEASE CANDIDATE: SUCCESS")
        print("Health:", health.json())
        print("Ready:", ready.json())
        print("KPIs Extended keys:", list(kpis.json().keys()))
        return 0
    finally:
        await pool.close()


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
