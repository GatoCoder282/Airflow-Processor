from __future__ import annotations

import logging

import redis.asyncio as redis
from redis.exceptions import ResponseError

from ..domain.models import DagCatalogSyncEvent
from ..services.event_parser import parse_catalog_sync_event

logger = logging.getLogger(__name__)


class RedisCatalogSyncConsumer:
    def __init__(
        self,
        client: redis.Redis,
        stream_catalog_sync: str,
        group_catalog_sync: str,
        consumer_name: str,
        block_ms: int = 5000,
        batch_count: int = 50,
    ):
        self._client = client
        self._stream_catalog_sync = stream_catalog_sync
        self._group_catalog_sync = group_catalog_sync
        self._consumer_name = consumer_name
        self._block_ms = block_ms
        self._batch_count = batch_count
        self._started = False

    async def setup(self) -> None:
        if self._started:
            return
        try:
            await self._client.xgroup_create(name=self._stream_catalog_sync, groupname=self._group_catalog_sync, id="0", mkstream=True)
        except ResponseError as exc:
            if "BUSYGROUP" not in str(exc):
                raise
        self._started = True

    async def read_events(self) -> list[tuple[str, DagCatalogSyncEvent]]:
        await self.setup()
        response = await self._client.xreadgroup(
            groupname=self._group_catalog_sync,
            consumername=self._consumer_name,
            streams={self._stream_catalog_sync: ">"},
            count=self._batch_count,
            block=self._block_ms,
        )
        events: list[tuple[str, DagCatalogSyncEvent]] = []
        for _, messages in response:
            for stream_id, fields in messages:
                try:
                    events.append((stream_id, parse_catalog_sync_event(stream_id, fields)))
                except Exception:
                    logger.exception("Error deserializando catalog sync %s", stream_id)
                    await self.acknowledge(stream_id)
        return events

    async def acknowledge(self, *msg_ids: str) -> None:
        if msg_ids:
            await self._client.xack(self._stream_catalog_sync, self._group_catalog_sync, *msg_ids)
