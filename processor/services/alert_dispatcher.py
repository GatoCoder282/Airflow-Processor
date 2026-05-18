from __future__ import annotations

import asyncio
import logging

from ..ports.alert_notifier import IAlertNotifier
from ..ports.monitoring_repository import IMonitoringRepository

logger = logging.getLogger(__name__)


class AlertDispatcher:
    def __init__(self, notifier: IAlertNotifier, repo: IMonitoringRepository, queue: asyncio.Queue):
        self._notifier = notifier
        self._repo = repo
        self._queue = queue
        self._metrics = {
            "alerts_sent": 0,
            "alerts_failed": 0,
            "dispatch_timeouts": 0,
        }

    def get_metrics(self) -> dict[str, int]:
        return {k: int(v) for k, v in self._metrics.items()}

    async def run(self) -> None:
        logger.info("AlertDispatcher iniciado")
        while True:
            try:
                alert_id, alert = await asyncio.wait_for(self._queue.get(), timeout=5.0)
                logger.debug("stage=notify status=start alert_id=%s", alert_id)
                success = await self._notifier.send(alert)
                if success:
                    await self._repo.mark_alert_notified(alert_id, self._notifier.channel_name)
                    self._metrics["alerts_sent"] += 1
                    logger.info("Alerta enviada: %s", alert.title)
                    logger.debug("stage=notify status=sent alert_id=%s channel=%s", alert_id, self._notifier.channel_name)
                else:
                    self._metrics["alerts_failed"] += 1
                    logger.warning("Fallo envio alerta: %s", alert.title)
                    logger.debug("stage=notify status=failed alert_id=%s", alert_id)
            except asyncio.TimeoutError:
                self._metrics["dispatch_timeouts"] += 1
                continue
            except asyncio.CancelledError:
                logger.info("AlertDispatcher detenido")
                break
            except Exception:
                logger.exception("Error en AlertDispatcher")