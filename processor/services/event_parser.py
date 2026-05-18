from datetime import datetime, timezone
import json

from ..domain.enums import EventType
from ..domain.models import AirflowEvent, DagCatalogSyncEvent, RawEvent


GLOBAL_DAG_IDS = {"scheduler", "system", "importer"}


def _as_text(value: str | None) -> str | None:
    if value is None:
        return None
    stripped = value.strip()
    return stripped or None


def _as_bool(value: str | None) -> bool:
    if value is None:
        return False
    return value.lower() in {"true", "1", "yes"}


def _as_int(value: str | None) -> int | None:
    if value is None or value == "":
        return None
    try:
        return int(value)
    except ValueError:
        return None


def _as_float(value: str | None) -> float | None:
    if value is None or value == "":
        return None
    try:
        return float(value)
    except ValueError:
        return None


def _as_datetime(value: str | None) -> datetime | None:
    if value is None or value == "":
        return None
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None


def _as_list_of_text(value: str | None) -> list[str] | None:
    normalized = _as_text(value)
    if normalized is None:
        return None

    try:
        parsed = json.loads(normalized)
        if isinstance(parsed, list):
            result = [str(item).strip() for item in parsed if str(item).strip()]
            return result or None
    except json.JSONDecodeError:
        pass

    parts = [part.strip() for part in normalized.split(",") if part.strip()]
    return parts or None


def _normalize_global_identity(event_type: EventType, dag_id: str, region: str) -> tuple[str, str]:
    if event_type == EventType.SCHEDULER_UNHEALTHY:
        return "scheduler", "global"

    normalized_region = region
    normalized_dag_id = dag_id

    if normalized_dag_id in GLOBAL_DAG_IDS and normalized_region != "global":
        normalized_region = "global"

    if event_type in {EventType.TASK_LOG, EventType.DAG_WARNING} and normalized_dag_id == "system":
        normalized_region = "global"

    if event_type in {EventType.IMPORT_ERROR, EventType.IMPORT_ERROR_DETECTED} and normalized_dag_id == "importer":
        normalized_region = "global"

    return normalized_dag_id, normalized_region


def parse_airflow_event(stream_id: str, fields: dict[str, str]) -> RawEvent:
    event_type_raw = _as_text(fields.get("event_type"))
    if event_type_raw is None:
        raise ValueError("event_type is required")

    try:
        event_type = EventType(event_type_raw)
    except ValueError as exc:
        raise ValueError(f"unsupported event_type: {event_type_raw}") from exc

    dag_id = _as_text(fields.get("dag_id"))
    if dag_id is None:
        if event_type in {EventType.TASK_LOG, EventType.DAG_WARNING}:
            dag_id = "system"
        elif event_type in {EventType.IMPORT_ERROR, EventType.IMPORT_ERROR_DETECTED}:
            dag_id = "importer"
        elif event_type == EventType.SCHEDULER_UNHEALTHY:
            dag_id = "scheduler"
        else:
            raise ValueError("dag_id is required")

    region = _as_text(fields.get("region")) or "BO"
    dag_id, region = _normalize_global_identity(event_type, dag_id, region)

    timestamp = _as_datetime(fields.get("timestamp")) or datetime.now(timezone.utc)
    return RawEvent(
        msg_id=stream_id,
        event_type=event_type,
        dag_id=dag_id,
        region=region,
        timestamp=timestamp,
        run_id=_as_text(fields.get("run_id")),
        run_state=_as_text(fields.get("run_state")),
        run_type=_as_text(fields.get("run_type")),
        execution_date=_as_datetime(fields.get("execution_date")),
        start_date=_as_datetime(fields.get("start_date")),
        end_date=_as_datetime(fields.get("end_date")),
        duration=_as_float(fields.get("duration")),
        task_id=_as_text(fields.get("task_id")),
        task_state=_as_text(fields.get("task_state")),
        upstream_task_id=_as_text(fields.get("upstream_task_id")),
        downstream_task_ids=_as_list_of_text(fields.get("downstream_task_ids")),
        try_number=_as_int(fields.get("try_number")),
        max_tries=_as_int(fields.get("max_tries")),
        sla_miss=_as_bool(fields.get("sla_miss")),
        reports_generated=_as_int(fields.get("reports_generated")),
        detail=_as_text(fields.get("detail")),
        log_excerpt=_as_text(fields.get("log_excerpt")) or (_as_text(fields.get("detail"))[:1000] if _as_text(fields.get("detail")) else None),
        last_log_token=_as_text(fields.get("last_log_token")) or _as_text(fields.get("continuation_token")),
    )


def parse_catalog_sync_event(stream_id: str, fields: dict[str, str]) -> DagCatalogSyncEvent:
    timestamp = _as_datetime(fields.get("ts")) or _as_datetime(fields.get("timestamp")) or datetime.now(timezone.utc)
    raw_changes = _as_text(fields.get("changes")) or ""
    changes: dict[str, object] = {}
    if raw_changes:
        # Redis Stream fields are flat strings; keep this safe and explicit.
        changes = {"raw": raw_changes}

    return DagCatalogSyncEvent(
        stream_id=stream_id,
        sync_type=_as_text(fields.get("sync_type")) or "unknown",
        dag_id=_as_text(fields.get("dag_id")) or "catalog",
        region=_as_text(fields.get("region")) or "global",
        changes=changes,
        timestamp=timestamp,
    )
