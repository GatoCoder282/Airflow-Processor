from __future__ import annotations

from datetime import datetime

from pydantic import BaseModel, Field

from .enums import (
    AlertSeverity,
    AlertType,
    EventType,
    IncidentCategory,
    NotificationChannel,
    Semaphore,
    SemaphoreColor,
    Severity,
)


class AirflowEvent(BaseModel):
    stream_id: str
    event_type: EventType
    dag_id: str
    region: str

    timestamp: datetime
    run_id: str | None = None
    run_state: str | None = None
    run_type: str | None = None
    execution_date: datetime | None = None
    start_date: datetime | None = None
    end_date: datetime | None = None
    duration: float | None = None

    task_id: str | None = None
    task_state: str | None = None
    try_number: int | None = None
    max_tries: int | None = None
    sla_miss: bool = False
    detail: str | None = None


class DagCatalogSyncEvent(BaseModel):
    stream_id: str
    sync_type: str
    dag_id: str
    region: str
    changes: dict[str, object] = Field(default_factory=dict)
    timestamp: datetime


class DagContext(BaseModel):
    criticality: str | None = None
    sla_seconds: int | None = None
    expected_reports_count: int | None = None


class ProcessingDecision(BaseModel):
    semaphore: Semaphore
    duration_deviation: float | None = None
    should_alert: bool = False
    alert_severity: Severity | None = None
    alert_type: str | None = None
    incident_category: str | None = None
    title: str | None = None
    message: str | None = None
    suppression_reason: str | None = None


class AlertMessage(BaseModel):
    alert_id: str
    region: str
    severity: Severity
    alert_type: str
    incident_category: str
    title: str
    message: str
    exception_snippet: str | None = None
    dag_id: str
    run_id: str | None = None
    task_id: str | None = None
    channels: list[str] = Field(default_factory=lambda: ["slack", "telegram"])
    ts: datetime


class RawEvent(BaseModel):
    msg_id: str
    event_type: EventType
    dag_id: str
    region: str
    timestamp: datetime
    run_id: str | None = None
    run_state: str | None = None
    run_type: str | None = None
    execution_date: datetime | None = None
    start_date: datetime | None = None
    end_date: datetime | None = None
    duration: float | None = None
    task_id: str | None = None
    task_state: str | None = None
    upstream_task_id: str | None = None
    downstream_task_ids: list[str] | None = None
    try_number: int | None = None
    max_tries: int | None = None
    sla_miss: bool = False
    detail: str | None = None
    log_excerpt: str | None = None
    last_log_token: str | None = None


class SemaphoreResult(BaseModel):
    color: SemaphoreColor
    reason: str
    avg_duration_ref: float | None = None
    duration_deviation: float | None = None


class ProcessedEvent(BaseModel):
    raw: RawEvent
    semaphore: SemaphoreResult
    dag_criticality: str | None = None
    dag_sla_seconds: int | None = None
    should_alert: bool = False
    alert: AlertToSend | None = None


class AlertToSend(BaseModel):
    dag_id: str
    region: str
    run_id: str | None = None
    task_id: str | None = None
    alert_type: AlertType
    severity: AlertSeverity
    incident_category: IncidentCategory
    title: str
    message: str
    exception_snippet: str | None = None
    event_type_source: str | None = None
    dedup_key: str | None = None
    root_cause_task_id: str | None = None
    id_report: int | None = None
    channels: list[str] = Field(default_factory=list)
    semaphore_reason: str | None = None
    active_task_id: str | None = None
    active_task_state: str | None = None
    start_date: datetime | None = None
    source_tag: str | None = None


__all__ = [
    "AirflowEvent",
    "AlertMessage",
    "AlertSeverity",
    "AlertToSend",
    "AlertType",
    "DagCatalogSyncEvent",
    "DagContext",
    "EventType",
    "IncidentCategory",
    "NotificationChannel",
    "ProcessingDecision",
    "ProcessedEvent",
    "RawEvent",
    "Semaphore",
    "SemaphoreColor",
    "SemaphoreResult",
    "Severity",
]
