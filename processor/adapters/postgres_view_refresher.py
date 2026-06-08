from __future__ import annotations

import asyncpg

from ..ports.view_refresher import IViewRefresher


class PostgresViewRefresher(IViewRefresher):
    def __init__(self, pool: asyncpg.Pool):
        self._pool = pool

    async def refresh_current_status(self) -> None:
        async with self._pool.acquire() as conn:
            await conn.execute("REFRESH MATERIALIZED VIEW CONCURRENTLY monitoring.dag_current_status")

    async def refresh_performance_stats(self) -> None:
        async with self._pool.acquire() as conn:
            await conn.execute("REFRESH MATERIALIZED VIEW CONCURRENTLY monitoring.dag_performance_stats")

    async def refresh_broken_url_summary(self) -> None:
        async with self._pool.acquire() as conn:
            await conn.execute("REFRESH MATERIALIZED VIEW CONCURRENTLY monitoring.broken_url_summary")

    async def refresh_broken_url_priority(self) -> None:
        async with self._pool.acquire() as conn:
            await conn.execute("REFRESH MATERIALIZED VIEW CONCURRENTLY monitoring.broken_url_priority")