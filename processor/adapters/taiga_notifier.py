from __future__ import annotations

import logging

import httpx

from ..domain.models import AlertToSend
from ..ports.alert_notifier import IAlertNotifier

logger = logging.getLogger(__name__)


class TaigaNotifier(IAlertNotifier):
    """Creates Taiga issues for incoming alerts.

    Requires:
        TAIGA_URL         - Base URL of your Taiga instance (e.g. https://taiga.mycompany.com)
        TAIGA_AUTH_TOKEN  - API token from Taiga Settings → Apps → User token
        TAIGA_PROJECT_ID  - Numeric ID of the Taiga project (visible in project settings URL)
    """

    channel_name = "taiga"

    def __init__(self, base_url: str, auth_token: str, project_id: int):
        self._base_url = base_url.rstrip("/")
        self._headers = {
            "Authorization": f"Bearer {auth_token}",
            "Content-Type": "application/json",
        }
        self._project_id = project_id
        # Cached project defaults — fetched lazily on first send
        self._default_priority: int | None = None
        self._default_status: int | None = None
        self._default_type: int | None = None

    async def _fetch_project_defaults(self) -> None:
        """Fetch default issue priority/status/type IDs for this project."""
        async with httpx.AsyncClient(timeout=10) as client:
            r = await client.get(
                f"{self._base_url}/api/v1/projects/{self._project_id}",
                headers=self._headers,
            )
            r.raise_for_status()
            data = r.json()
            self._default_priority = data.get("default_priority")
            self._default_status = data.get("default_issue_status")
            self._default_type = data.get("default_issue_type")
            logger.debug(
                "Taiga project defaults: priority=%s status=%s type=%s",
                self._default_priority, self._default_status, self._default_type,
            )

    async def send(self, alert: AlertToSend) -> bool:
        """Create a Taiga issue for this alert."""
        if self._default_priority is None:
            try:
                await self._fetch_project_defaults()
            except Exception:
                logger.exception("Taiga: failed to fetch project defaults for project_id=%s", self._project_id)
                return False

        subject = f"[{alert.severity.value.upper()}] {alert.title}"

        desc_lines = [
            f"**DAG:** {alert.dag_id}",
            f"**Región:** {alert.region}",
            f"**Tipo de alerta:** {alert.alert_type.value}",
            f"**Categoría:** {alert.incident_category.value}",
        ]
        if alert.run_id:
            desc_lines.append(f"**Run:** {alert.run_id}")
        if alert.semaphore_reason:
            desc_lines.append(f"**Semáforo (razón):** {alert.semaphore_reason}")
        if alert.active_task_id:
            state_label = alert.active_task_state or "?"
            desc_lines.append(f"**Última task:** {alert.active_task_id} ({state_label})")
        if alert.root_cause_task_id:
            desc_lines.append(f"**Root cause:** {alert.root_cause_task_id}")
        desc_lines += ["", "---", "", alert.message]

        payload: dict[str, object] = {
            "project": self._project_id,
            "subject": subject,
            "description": "\n".join(desc_lines),
            "tags": ["airflow", alert.severity.value, alert.dag_id, alert.alert_type.value],
        }
        if self._default_priority is not None:
            payload["priority"] = self._default_priority
        if self._default_status is not None:
            payload["status"] = self._default_status
        if self._default_type is not None:
            payload["type"] = self._default_type

        try:
            async with httpx.AsyncClient(timeout=10) as client:
                r = await client.post(
                    f"{self._base_url}/api/v1/issues",
                    headers=self._headers,
                    json=payload,
                )
            if r.status_code in (200, 201):
                issue_ref = r.json().get("ref", "?")
                logger.info("Taiga issue #%s created for dag=%s", issue_ref, alert.dag_id)
                return True
            logger.warning("Taiga issue creation failed: %s %s", r.status_code, r.text[:300])
            return False
        except Exception:
            logger.exception("Taiga send failed for dag=%s", alert.dag_id)
            return False
