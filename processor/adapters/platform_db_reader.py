from __future__ import annotations

import logging

import asyncpg

logger = logging.getLogger(__name__)


class PlatformDbReader:
    """Lecturas (solo lectura) a la base operacional ``platform_db`` (schema ``public``).

    Encapsula la segunda fuente de datos y la **tolerancia a fallos**: si el pool no está
    configurado (DSN vacío) o la base está caída / la query falla, cada método devuelve un
    valor vacío seguro en vez de propagar el error. Así los endpoints que enriquecen nunca
    rompen su contrato por culpa de platform_db.
    """

    def __init__(self, pool: asyncpg.Pool | None):
        self._pool = pool

    @property
    def available(self) -> bool:
        return self._pool is not None

    async def _fetch(self, query: str, *args: object) -> list[dict]:
        if self._pool is None:
            return []
        try:
            async with self._pool.acquire() as conn:
                rows = await conn.fetch(query, *args)
                return [dict(r) for r in rows]
        except Exception:
            logger.warning("platform_db query failed", exc_info=True)
            return []

    async def file_enrichment(self) -> dict[str, dict]:
        """Devuelve ``{dag_id: {name, main_url, path, updated_to}}`` de toda la tabla file.

        ``file.code`` == ``dag_id`` del DAG de descarga. Tabla modesta (~1 fila por DAG).
        """
        rows = await self._fetch(
            """
            SELECT code           AS dag_id,
                   name           AS name,
                   main_url       AS main_url,
                   last_file_path AS path,
                   updated_to     AS updated_to
            FROM public.file
            WHERE code IS NOT NULL
            """
        )
        return {r["dag_id"]: r for r in rows if r.get("dag_id")}

    async def broken_urls_for(self, dag_ids: list[str]) -> dict[str, dict]:
        """Para los ``dag_ids`` dados, ``{dag_id: {file_name, path, datos_a, main_url, broken_url}}``.

        La URL rota es la última registrada por ``file_code`` (DISTINCT ON + execution_date DESC).
        """
        if not dag_ids:
            return {}
        rows = await self._fetch(
            """
            SELECT f.code           AS dag_id,
                   f.name           AS file_name,
                   f.last_file_path AS path,
                   f.updated_to     AS datos_a,
                   f.main_url       AS main_url,
                   bu.url           AS broken_url
            FROM public.file f
            LEFT JOIN (
                SELECT DISTINCT ON (file_code) file_code, url
                FROM public.broken_url
                ORDER BY file_code, execution_date DESC
            ) bu ON bu.file_code = f.code
            WHERE f.code = ANY($1::text[])
            """,
            dag_ids,
        )
        return {r["dag_id"]: r for r in rows if r.get("dag_id")}

    async def reports_by_dag(self, dag_id: str) -> list[dict]:
        """Reportes generados por el archivo del DAG y los cubos/bases que alimentan."""
        return await self._fetch(
            """
            SELECT r.code          AS report_code,
                   r.name          AS report_name,
                   r.type          AS report_type,
                   r.converted_to  AS converted_to,
                   r."isActive"    AS is_active,
                   r.file_extension,
                   db.db_code,
                   db.name         AS database_name,
                   db.updated_to   AS db_updated_to,
                   db.data_frequency
            FROM public.file f
            JOIN public.report r ON r.id_file = f.id_file
            LEFT JOIN public.data_base_report dbr ON dbr.report_code = r.code
            LEFT JOIN public.data_base db ON db.db_code = dbr.db_code
            WHERE f.code = $1
            ORDER BY r.code, db.db_code
            """,
            dag_id,
        )

    async def close(self) -> None:
        if self._pool is not None:
            await self._pool.close()
