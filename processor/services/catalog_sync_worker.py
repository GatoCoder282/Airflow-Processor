from __future__ import annotations

import asyncio
import logging

from ..adapters.redis_catalog_consumer import RedisCatalogSyncConsumer
from ..ports.monitoring_repository import IMonitoringRepository

logger = logging.getLogger(__name__)


class CatalogSyncWorker:
    def __init__(self, consumer: RedisCatalogSyncConsumer, repository: IMonitoringRepository):
        self._consumer = consumer
        self._repository = repository
        self._running = False

    async def run(self) -> None:
        self._running = True
        logger.info("CatalogSyncWorker iniciado")
        while self._running:
            try:
                events = await self._consumer.read_events()
                for stream_id, event in events:
                    await self._repository.update_catalog_from_sync(event)
                    await self._consumer.acknowledge(stream_id)
            except asyncio.CancelledError:
                logger.info("CatalogSyncWorker detenido")
                break
            except Exception:
                logger.exception("Error en CatalogSyncWorker")
                await asyncio.sleep(1)
