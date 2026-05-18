from __future__ import annotations

import logging

import redis.asyncio as aioredis
from redis.exceptions import ResponseError

from ..domain.models import RawEvent
from ..ports.event_consumer import IEventConsumer
from ..services.event_parser import parse_airflow_event

logger = logging.getLogger(__name__)


class RedisConsumer(IEventConsumer):
    STREAM = "stream:airflow_events"
    GROUP = "backend_processors"
    CONSUMER = "backend_worker_1"

    def __init__(
        self,
        host: str,
        port: int,
        password: str | None = None,
        db: int = 0,
        stream: str = STREAM,
        group: str = GROUP,
        consumer_name: str = CONSUMER,
    ):
        self._redis = aioredis.Redis(host=host, port=port, password=password, db=db, decode_responses=True)
        self._stream = stream
        self._group = group
        self._consumer_name = consumer_name
        self._started = False
        self._metrics = {
            "consumed_batches": 0,
            "consumed_events": 0,
            "parse_errors": 0,
            "acknowledged_events": 0,
        }

    def get_metrics(self) -> dict[str, int]:
        return {k: int(v) for k, v in self._metrics.items()}

    async def setup(self) -> None:
        if self._started:
            return
        try:
            await self._redis.xgroup_create(self._stream, self._group, id="0", mkstream=True)
        except ResponseError as exc:
            if "BUSYGROUP" not in str(exc):
                raise
        self._started = True

    async def read_events(self, count: int = 10) -> list[RawEvent]:
        await self.setup()
        result = await self._redis.xreadgroup(
            groupname=self._group,
            consumername=self._consumer_name,
            streams={self._stream: ">"},
            count=count,
            block=5000,
        )
        if not result:
            return []
        self._metrics["consumed_batches"] += 1
        events: list[RawEvent] = []
        for _, messages in result:
            for msg_id, data in messages:
                try:
                    event = parse_airflow_event(msg_id, data)
                    event.msg_id = msg_id
                    events.append(event)
                    self._metrics["consumed_events"] += 1
                    logger.debug(
                        "stage=parse status=ok stream=%s group=%s msg_id=%s event_type=%s dag_id=%s",
                        self._stream,
                        self._group,
                        msg_id,
                        event.event_type.value,
                        event.dag_id,
                    )
                except Exception:
                    self._metrics["parse_errors"] += 1
                    logger.exception("Error deserializando evento %s", msg_id)
                    await self.acknowledge(msg_id)
        return events

    async def acknowledge(self, *msg_ids: str) -> None:
        if msg_ids:
            await self._redis.xack(self._stream, self._group, *msg_ids)
            self._metrics["acknowledged_events"] += len(msg_ids)

    async def get_pending_count(self) -> int:
        pending = await self._redis.xpending(self._stream, self._group)
        return int(pending.get("pending", 0))

    async def close(self) -> None:
        await self._redis.aclose()