from __future__ import annotations

from fastapi import APIRouter, Request

router = APIRouter(tags=["health"])


@router.get("/health")
async def health(request: Request):
    factory = request.app.state.factory
    event_processor_metrics = factory.event_processor.get_metrics() if factory.event_processor is not None else {}
    dispatcher_metrics = factory.alert_dispatcher.get_metrics() if factory.alert_dispatcher is not None else {}
    scheduler_metrics = factory.view_scheduler.get_metrics() if factory.view_scheduler is not None else {}
    consumer_metrics = {}
    if factory.event_processor is not None and hasattr(factory.event_processor, "_consumer"):
        consumer = factory.event_processor._consumer
        consumer_metrics = consumer.get_metrics() if hasattr(consumer, "get_metrics") else {}
    alert_queue_size = 0
    if factory.event_processor is not None and hasattr(factory.event_processor, "_alert_queue"):
        alert_queue_size = int(factory.event_processor._alert_queue.qsize())

    return {
        "status": "ok",
        "service": factory.config.app_name,
        "components": {
            "event_processor": "running" if factory.event_processor is not None else "missing",
            "alert_dispatcher": "running" if factory.alert_dispatcher is not None else "missing",
            "view_scheduler": "running" if factory.view_scheduler is not None else "missing",
            "catalog_sync_worker": "running" if factory.catalog_sync_worker is not None else "missing",
        },
        "queues": {
            "alert_queue_size": alert_queue_size,
        },
        "metrics": {
            "consumer": consumer_metrics,
            "event_processor": event_processor_metrics,
            "alert_dispatcher": dispatcher_metrics,
            "view_scheduler": scheduler_metrics,
        },
    }


@router.get("/ready")
async def ready(request: Request):
    factory = request.app.state.factory
    redis_status = "unknown"
    postgres_status = "unknown"
    redis_error = None
    postgres_error = None

    if factory.redis_client is not None:
        try:
            await factory.redis_client.ping()
            redis_status = "ready"
        except Exception as exc:
            redis_status = "error"
            redis_error = str(exc)

    if factory.db_pool is not None:
        try:
            async with factory.db_pool.acquire() as conn:
                await conn.fetchval("SELECT 1")
            postgres_status = "ready"
        except Exception as exc:
            postgres_status = "error"
            postgres_error = str(exc)

    status = "ready" if redis_status == "ready" and postgres_status == "ready" else "degraded"
    return {
        "status": status,
        "components": {
            "redis": {"status": redis_status, "error": redis_error},
            "postgres": {"status": postgres_status, "error": postgres_error},
        },
    }


@router.get("/health/refresh-metrics")
async def refresh_metrics(request: Request):
    factory = request.app.state.factory
    scheduler = factory.view_scheduler
    metrics = scheduler.get_metrics() if scheduler is not None else {}
    return {"status": "ok", "metrics": metrics}