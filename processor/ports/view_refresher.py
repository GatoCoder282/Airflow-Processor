from __future__ import annotations

from abc import ABC, abstractmethod


class IViewRefresher(ABC):
    @abstractmethod
    async def refresh_current_status(self) -> None:
        ...

    @abstractmethod
    async def refresh_performance_stats(self) -> None:
        ...

    @abstractmethod
    async def refresh_broken_url_summary(self) -> None:
        ...

    @abstractmethod
    async def refresh_broken_url_priority(self) -> None:
        ...