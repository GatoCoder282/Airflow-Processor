from __future__ import annotations

import logging

import httpx

from ..domain.enums import AlertSeverity
from ..domain.models import AlertToSend
from ..ports.alert_notifier import IAlertNotifier

logger = logging.getLogger(__name__)


class SlackNotifier(IAlertNotifier):
    channel_name = "slack"

    def __init__(self, webhook_url: str):
        self._webhook = webhook_url

    async def send(self, alert: AlertToSend) -> bool:
        emoji = {"critical": ":red_circle:", "warning": ":yellow_circle:", "info": ":white_circle:"}
        payload = {
            "text": f'{emoji.get(alert.severity.value, ":white_circle:")} *{alert.title}*',
            "attachments": [
                {
                    "color": "#FF0000" if alert.severity == AlertSeverity.CRITICAL else "#FFA500",
                    "fields": [
                        {"title": "DAG", "value": alert.dag_id, "short": True},
                        {"title": "Region", "value": alert.region, "short": True},
                        {"title": "Tipo", "value": alert.alert_type.value, "short": True},
                        {"title": "Categoria", "value": alert.incident_category.value, "short": True},
                        {"title": "Mensaje", "value": alert.message, "short": False},
                    ],
                }
            ],
        }
        if alert.exception_snippet:
            payload["attachments"][0]["fields"].append({"title": "Error", "value": f"```{alert.exception_snippet}```", "short": False})
        try:
            async with httpx.AsyncClient(timeout=10) as client:
                response = await client.post(self._webhook, json=payload)
            return response.status_code == 200
        except Exception:
            logger.exception("Slack send failed")
            return False