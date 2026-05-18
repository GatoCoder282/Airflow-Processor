from __future__ import annotations

import asyncio
import logging
import time
from collections.abc import Callable
from typing import Awaitable

from ..ports.view_refresher import IViewRefresher

logger = logging.getLogger(__name__)


class ViewScheduler:
    INTERVALS = {
        "current_status": 15,
        "performance_stats": 300,
        "broken_url": 600,
        "incidence_timeline": 300,
        "broken_url_priority": 600,
        "stale_run_check": 600,
    }

    def __init__(
        self,
        refresher: IViewRefresher,
        refresh_current_status_seconds: int = INTERVALS["current_status"],
        refresh_performance_seconds: int = INTERVALS["performance_stats"],
        refresh_broken_url_seconds: int = INTERVALS["broken_url"],
        refresh_incidence_timeline_seconds: int = INTERVALS["incidence_timeline"],
        refresh_broken_url_priority_seconds: int = INTERVALS["broken_url_priority"],
        stale_run_checker: Callable[[], Awaitable[None]] | None = None,
        stale_run_check_seconds: int = INTERVALS["stale_run_check"],
    ):
        self._refresher = refresher
        self._stale_checker = stale_run_checker
        self._intervals = {
            "current_status": refresh_current_status_seconds,
            "performance_stats": refresh_performance_seconds,
            "broken_url": refresh_broken_url_seconds,
            "incidence_timeline": refresh_incidence_timeline_seconds,
            "broken_url_priority": refresh_broken_url_priority_seconds,
            "stale_run_check": stale_run_check_seconds,
        }
        self._last_run: dict[str, float] = {}
        self._metrics: dict[str, dict[str, object]] = {
            name: {
                "success_count": 0,
                "error_count": 0,
                "last_duration_ms": None,
                "last_error": None,
                "last_run_at": None,
            }
            for name in self._intervals
        }

    async def run(self) -> None:
        logger.info("ViewScheduler iniciado")
        while True:
            try:
                now = time.monotonic()
                await self._maybe_refresh("current_status", self._refresher.refresh_current_status, now)
                await self._maybe_refresh("performance_stats", self._refresher.refresh_performance_stats, now)
                await self._maybe_refresh("broken_url", self._refresher.refresh_broken_url_summary, now)
                await self._maybe_refresh("incidence_timeline", self._refresher.refresh_incidence_timeline, now)
                await self._maybe_refresh("broken_url_priority", self._refresher.refresh_broken_url_priority, now)
                if self._stale_checker:
                    await self._maybe_refresh("stale_run_check", self._stale_checker, now)
                await asyncio.sleep(1)
            except asyncio.CancelledError:
                logger.info("ViewScheduler detenido")
                break
            except Exception:
                logger.exception("Error en ViewScheduler")

    async def _maybe_refresh(self, name: str, fn, now: float) -> None:
        last = self._last_run.get(name, 0)
        if now - last >= self._intervals[name]:
            started = time.perf_counter()
            try:
                await fn()
                elapsed_ms = (time.perf_counter() - started) * 1000.0
                self._last_run[name] = now
                self._metrics[name]["success_count"] = int(self._metrics[name]["success_count"]) + 1
                self._metrics[name]["last_duration_ms"] = round(elapsed_ms, 2)
                self._metrics[name]["last_error"] = None
                self._metrics[name]["last_run_at"] = time.time()
                logger.debug("Vista %s refrescada en %.2f ms", name, elapsed_ms)
            except Exception as exc:
                self._metrics[name]["error_count"] = int(self._metrics[name]["error_count"]) + 1
                self._metrics[name]["last_error"] = str(exc)
                self._metrics[name]["last_run_at"] = time.time()
                logger.exception("Error refrescando vista %s", name)

    def get_metrics(self) -> dict[str, dict[str, object]]:
        return {name: dict(values) for name, values in self._metrics.items()}