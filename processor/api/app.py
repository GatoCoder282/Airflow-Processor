from __future__ import annotations

import asyncio
import logging
from contextlib import asynccontextmanager

from dotenv import load_dotenv
from fastapi import FastAPI

load_dotenv()

from ..infrastructure.config import BackendConfig
from ..infrastructure.factory import BackendFactory
from ..infrastructure.logging import configure_logging
from .router import router

logger = logging.getLogger(__name__)


def create_app() -> FastAPI:
    settings = BackendConfig.from_env()
    configure_logging(settings.app_log_level)

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        factory = await BackendFactory.create(settings)
        app.state.factory = factory
        tasks = [
            asyncio.create_task(factory.event_processor.run(), name="event_processor"),
            asyncio.create_task(factory.alert_dispatcher.run(), name="alert_dispatcher"),
            asyncio.create_task(factory.view_scheduler.run(), name="view_scheduler"),
            asyncio.create_task(factory.catalog_sync_worker.run(), name="catalog_sync_worker"),
        ]
        logger.info("Backend started")
        try:
            yield
        finally:
            for task in tasks:
                task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)
            await factory.close()
            logger.info("Backend stopped")

    app = FastAPI(title=settings.app_name, lifespan=lifespan)
    app.include_router(router)
    return app
