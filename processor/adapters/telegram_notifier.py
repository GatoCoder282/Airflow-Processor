from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone

import httpx

from ..domain.enums import AlertSeverity
from ..domain.models import AlertToSend
from ..ports.alert_notifier import IAlertNotifier

logger = logging.getLogger(__name__)

_BOL_TZ = timezone(timedelta(hours=-4))
_DAYS_ES = ["Lun", "Mar", "Mié", "Jue", "Vie", "Sáb", "Dom"]


def _h(value: str) -> str:
    """Escape HTML special chars for Telegram HTML parse mode."""
    return value.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")


def _fmt_dt(value: datetime | str | None) -> str:
    """Format datetime to Bolivia time as 'Lun 28/04 · 11:03'."""
    if value is None:
        return "—"
    if isinstance(value, str):
        try:
            dt = datetime.fromisoformat(value)
        except ValueError:
            return value
    else:
        dt = value
    if dt.tzinfo is not None:
        dt = dt.astimezone(_BOL_TZ)
    else:
        dt = dt.replace(tzinfo=timezone.utc).astimezone(_BOL_TZ)
    return f"{_DAYS_ES[dt.weekday()]} {dt.day:02d}/{dt.month:02d} · {dt.hour:02d}:{dt.minute:02d}"


class TelegramNotifier(IAlertNotifier):
    channel_name = "telegram"
    _BASE_URL = "https://api.telegram.org/bot{token}/sendMessage"

    def __init__(self, bot_token: str, chat_id: str):
        self._token = bot_token
        self._chat_id = chat_id

    async def send(self, alert: AlertToSend) -> bool:
        """Send alert to Telegram for all failed DAG alerts."""
        # if alert.severity != AlertSeverity.CRITICAL:
        #     return True  # silently skip warning/info

        lines = [
            f"🔴 <b>DAG FALLIDO: {_h(alert.dag_id)}</b>",
            "━━━━━━━━━━━━━━━━━━━━━",
            f"🌍 Región: <code>{_h(alert.region)}</code>",
            f"📋 Tipo: <code>{alert.alert_type.value}</code>",
        ]
        if alert.run_id:
            lines.append(f"🔗 Run: <code>{_h(alert.run_id)}</code>")
        if alert.start_date:
            lines.append(f"📅 Fecha: <code>{_fmt_dt(alert.start_date)}</code>")
        if alert.source_tag:
            lines.append(f"🏭 Fuente: <code>{_h(alert.source_tag)}</code>")
        if alert.cube_tag:
            lines.append(f"📦 Cubo: <code>{_h(alert.cube_tag)}</code>")
        if alert.semaphore_reason:
            lines.append(f"💬 Semáforo: <code>{_h(alert.semaphore_reason)}</code>")
        if alert.active_task_id:
            state_label = alert.active_task_state or "?"
            lines.append(f"🎯 Última task: <code>{_h(alert.active_task_id)}</code> → <code>{state_label}</code>")
        if alert.root_cause_task_id and alert.root_cause_task_id != alert.active_task_id:
            lines.append(f"🕵️ Root cause: <code>{_h(alert.root_cause_task_id)}</code>")
        lines += [
            "━━━━━━━━━━━━━━━━━━━━━",
            _h(alert.message),
        ]
        return await self._post("\n".join(lines))

    async def notify_status_change(
        self,
        alert_id: int,
        action: str,
        dag_id: str,
        title: str,
        actor: str | None = None,
        suppression_reason: str | None = None,
    ) -> bool:
        """Notify when a dashboard action is taken: resolved, acknowledged, suppressed."""
        icons = {"resolved": "✅", "acknowledged": "👁", "suppressed": "🔕"}
        labels = {"resolved": "RESUELTA", "acknowledged": "RECONOCIDA", "suppressed": "SUPRIMIDA"}
        icon = icons.get(action, "ℹ️")
        label = labels.get(action, action.upper())

        lines = [
            f"{icon} <b>Alerta {label}</b>",
            f"DAG: <code>{_h(dag_id)}</code>",
            f"<i>{_h(title)}</i>",
        ]
        if actor:
            lines.append(f"Por: <code>{_h(actor)}</code>")
        if suppression_reason:
            lines.append(f"Razón: <code>{_h(suppression_reason)}</code>")
        lines.append(f"ID: #{alert_id}")

        return await self._post("\n".join(lines))

    async def _post(self, text: str) -> bool:
        try:
            url = self._BASE_URL.format(token=self._token)
            async with httpx.AsyncClient(timeout=10) as client:
                response = await client.post(
                    url,
                    json={"chat_id": self._chat_id, "text": text, "parse_mode": "HTML"},
                )
            if response.status_code != 200:
                logger.warning("Telegram responded %s: %s", response.status_code, response.text)
            return response.status_code == 200
        except Exception:
            logger.exception("Telegram send failed")
            return False
