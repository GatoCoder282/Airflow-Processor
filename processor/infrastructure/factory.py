from __future__ import annotations

import asyncio

import sys

import asyncpg
import redis.asyncio as redis

from ..adapters.composite_notifier import CompositeNotifier
from ..adapters.null_notifier import NullNotifier
from ..adapters.postgres_monitoring_repo import PostgresMonitoringRepository
from ..adapters.postgres_view_refresher import PostgresViewRefresher
from ..adapters.redis_catalog_consumer import RedisCatalogSyncConsumer
from ..adapters.redis_consumer import RedisConsumer
from ..adapters.taiga_notifier import TaigaNotifier
from ..adapters.telegram_notifier import TelegramNotifier
from ..domain.models import AlertToSend
from ..ports.alert_notifier import IAlertNotifier
from ..services.catalog_sync_worker import CatalogSyncWorker
from ..services.alert_dispatcher import AlertDispatcher
from ..services.alert_evaluator import AlertEvaluator
from ..services.event_processor import EventProcessor
from ..services.semaphore_calculator import SemaphoreCalculator
from ..services.view_scheduler import ViewScheduler
from .config import BackendConfig


class BackendFactory:
    def __init__(self, config: BackendConfig):
        self.config = config
        self.db_pool: asyncpg.Pool | None = None
        self.redis_client: redis.Redis | None = None
        self.event_processor: EventProcessor | None = None
        self.alert_dispatcher: AlertDispatcher | None = None
        self.view_scheduler: ViewScheduler | None = None
        self.catalog_sync_worker: CatalogSyncWorker | None = None
        self._notifier: IAlertNotifier = NullNotifier()

    @classmethod
    async def create(cls, config: BackendConfig) -> "BackendFactory":
        factory = cls(config)

        print(f"[DEBUG] DSN: {config.postgres_dsn}", file=sys.stderr, flush=True)

        factory.db_pool = await asyncpg.create_pool(
            dsn=config.postgres_dsn,
            min_size=1,
            max_size=config.pg_pool_max,
            ssl=False,
        )
        factory.redis_client = redis.Redis(
            host=config.redis_host,
            port=config.redis_port,
            password=config.redis_password,
            db=config.redis_db,
            decode_responses=True,
        )

        consumer = RedisConsumer(
            host=config.redis_host,
            port=config.redis_port,
            password=config.redis_password,
            db=config.redis_db,
            stream=config.stream_airflow_events,
            group=config.group_backend_processors,
            consumer_name=config.consumer_name,
        )
        catalog_consumer = RedisCatalogSyncConsumer(
            factory.redis_client,
            config.stream_catalog_sync,
            config.group_catalog_sync,
            config.consumer_name,
        )
        repository = PostgresMonitoringRepository(factory.db_pool)
        refresher = PostgresViewRefresher(factory.db_pool)
        factory._notifier = factory._build_notifier(config)

        alert_queue: asyncio.Queue[tuple[int, AlertToSend]] = asyncio.Queue(maxsize=1000)
        calculator = SemaphoreCalculator()
        evaluator = AlertEvaluator()

        factory.event_processor = EventProcessor(
            consumer,
            repository,
            calculator,
            evaluator,
            alert_queue,
            alert_grace_seconds=config.alert_grace_seconds,
        )
        factory.alert_dispatcher = AlertDispatcher(factory._notifier, repository, alert_queue)
        factory.view_scheduler = ViewScheduler(
            refresher,
            refresh_current_status_seconds=config.refresh_current_status_seconds,
            refresh_performance_seconds=config.refresh_performance_seconds,
            refresh_broken_url_seconds=config.refresh_broken_url_seconds,
            refresh_incidence_timeline_seconds=config.refresh_incidence_timeline_seconds,
            refresh_broken_url_priority_seconds=config.refresh_broken_url_priority_seconds,
            stale_run_checker=repository.resolve_stale_running_runs,
        )
        factory.catalog_sync_worker = CatalogSyncWorker(catalog_consumer, repository)
        return factory

    @property
    def notifier(self) -> IAlertNotifier:
        return self._notifier

    def _build_notifier(self, config: BackendConfig) -> IAlertNotifier:
        notifiers: list[IAlertNotifier] = []
        if config.telegram_bot_token and config.telegram_chat_id:
            notifiers.append(TelegramNotifier(config.telegram_bot_token, config.telegram_chat_id))
        if config.taiga_url and config.taiga_auth_token and config.taiga_project_id:
            notifiers.append(TaigaNotifier(config.taiga_url, config.taiga_auth_token, config.taiga_project_id))
        if not notifiers:
            return NullNotifier()
        if len(notifiers) == 1:
            return notifiers[0]
        return CompositeNotifier(notifiers)

    async def close(self) -> None:
        if self.redis_client is not None:
            await self.redis_client.aclose()
        if self.db_pool is not None:
            await self.db_pool.close()
