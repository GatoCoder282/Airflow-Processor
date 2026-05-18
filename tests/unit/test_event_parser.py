import pytest

from processor.services.event_parser import parse_airflow_event


def test_parse_airflow_event_handles_optional_types() -> None:
    event = parse_airflow_event(
        stream_id="1700000-0",
        fields={
            "event_type": "task_state_change",
            "dag_id": "D_BO_0001",
            "region": "BO",
            "timestamp": "2026-03-24T14:05:33.102Z",
            "run_id": "scheduled__2026-03-24T14:00:00+00:00",
            "task_id": "download_file",
            "task_state": "failed",
            "try_number": "3",
            "max_tries": "3",
            "sla_miss": "true",
            "duration": "331.867",
        },
    )

    assert event.try_number == 3
    assert event.max_tries == 3
    assert event.sla_miss is True
    assert event.duration == 331.867
    assert event.task_state == "failed"


def test_parse_airflow_event_supports_log_warning_detail_types() -> None:
    event = parse_airflow_event(
        stream_id="1700001-0",
        fields={
            "event_type": "task_log",
            "dag_id": "D_BO_0002",
            "region": "BO",
            "detail": "Traceback: some failure detail",
            "timestamp": "2026-03-24T15:10:00.000Z",
        },
    )

    assert event.event_type.value == "task_log"
    assert event.detail == "Traceback: some failure detail"


def test_parse_airflow_event_supports_dag_run_state_change() -> None:
    event = parse_airflow_event(
        stream_id="1700002-0",
        fields={
            "event_type": "dag_run_state_change",
            "dag_id": "D_BO_1000",
            "region": "BO",
            "run_id": "run_123",
            "run_state": "running",
            "duration": "120.5",
        },
    )

    assert event.event_type.value == "dag_run_state_change"
    assert event.run_id == "run_123"
    assert event.run_state == "running"
    assert event.duration == 120.5


def test_parse_airflow_event_supports_dag_warning() -> None:
    event = parse_airflow_event(
        stream_id="1700003-0",
        fields={
            "event_type": "dag_warning",
            "dag_id": "D_BO_2000",
            "region": "BO",
            "detail": "Dag warning detail",
        },
    )

    assert event.event_type.value == "dag_warning"
    assert event.detail == "Dag warning detail"


def test_parse_airflow_event_supports_import_error_detected() -> None:
    event = parse_airflow_event(
        stream_id="1700004-0",
        fields={
            "event_type": "import_error_detected",
            "dag_id": "D_BO_3000",
            "region": "BO",
            "detail": "ImportError: module not found",
        },
    )

    assert event.event_type.value == "import_error_detected"
    assert event.detail == "ImportError: module not found"


def test_parse_airflow_event_normalizes_scheduler_unhealthy_global_identity() -> None:
    event = parse_airflow_event(
        stream_id="1700005-0",
        fields={
            "event_type": "scheduler_unhealthy",
            "dag_id": "any_value",
            "region": "BO",
        },
    )

    assert event.event_type.value == "scheduler_unhealthy"
    assert event.dag_id == "scheduler"
    assert event.region == "global"


def test_parse_airflow_event_normalizes_global_region_for_system_dag() -> None:
    event = parse_airflow_event(
        stream_id="1700006-0",
        fields={
            "event_type": "task_log",
            "dag_id": "system",
            "region": "BO",
        },
    )

    assert event.dag_id == "system"
    assert event.region == "global"


def test_parse_airflow_event_invalid_event_type_raises_value_error() -> None:
    with pytest.raises(ValueError, match="unsupported event_type"):
        parse_airflow_event(
            stream_id="1700007-0",
            fields={
                "event_type": "unknown_event",
                "dag_id": "D_BO_9999",
                "region": "BO",
            },
        )


def test_parse_airflow_event_parses_lineage_and_log_context() -> None:
    event = parse_airflow_event(
        stream_id="1700008-0",
        fields={
            "event_type": "task_log",
            "dag_id": "D_BO_4000",
            "region": "BO",
            "task_id": "transform",
            "upstream_task_id": "extract",
            "downstream_task_ids": '["load","validate"]',
            "detail": "x" * 1200,
            "continuation_token": "next-token",
        },
    )

    assert event.upstream_task_id == "extract"
    assert event.downstream_task_ids == ["load", "validate"]
    assert event.log_excerpt is not None
    assert len(event.log_excerpt) == 1000
    assert event.last_log_token == "next-token"
