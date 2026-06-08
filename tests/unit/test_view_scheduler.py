from __future__ import annotations

import pytest

from processor.services.view_scheduler import ViewScheduler


class FakeRefresher:
    def __init__(self) -> None:
        self.calls: list[str] = []
        self.fail_next = False

    async def refresh_current_status(self) -> None:
        self.calls.append("current_status")

    async def refresh_performance_stats(self) -> None:
        self.calls.append("performance_stats")
        if self.fail_next:
            self.fail_next = False
            raise RuntimeError("boom")

    async def refresh_broken_url_summary(self) -> None:
        self.calls.append("broken_url")

    async def refresh_broken_url_priority(self) -> None:
        self.calls.append("broken_url_priority")


@pytest.mark.asyncio
async def test_scheduler_tracks_refresh_latency_and_success_metrics() -> None:
    refresher = FakeRefresher()
    scheduler = ViewScheduler(refresher)

    await scheduler._maybe_refresh("current_status", refresher.refresh_current_status, now=1000.0)
    await scheduler._maybe_refresh("broken_url_priority", refresher.refresh_broken_url_priority, now=1000.0)

    metrics = scheduler.get_metrics()
    assert metrics["current_status"]["success_count"] == 1
    assert metrics["current_status"]["error_count"] == 0
    assert metrics["current_status"]["last_duration_ms"] is not None
    assert metrics["broken_url_priority"]["success_count"] == 1


@pytest.mark.asyncio
async def test_scheduler_tracks_refresh_errors_without_crashing() -> None:
    refresher = FakeRefresher()
    refresher.fail_next = True
    scheduler = ViewScheduler(refresher)

    await scheduler._maybe_refresh("performance_stats", refresher.refresh_performance_stats, now=1000.0)

    metrics = scheduler.get_metrics()
    assert metrics["performance_stats"]["error_count"] == 1
    assert metrics["performance_stats"]["last_error"] == "boom"
