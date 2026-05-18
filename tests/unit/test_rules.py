from datetime import datetime, timezone

from processor.domain.models import AirflowEvent, DagContext
from processor.domain.rules import evaluate_event


def _base_event() -> AirflowEvent:
    return AirflowEvent(
        stream_id="1-0",
        event_type="task_state_change",
        dag_id="D_BO_0001",
        region="BO",
        timestamp=datetime.now(timezone.utc),
        run_id="run_1",
        task_id="download",
        task_state="success",
    )


def test_rule_retry_exceeded_is_critical() -> None:
    event = _base_event().model_copy(update={"task_state": "failed", "try_number": 3, "max_tries": 3})
    decision = evaluate_event(event, DagContext(expected_reports_count=1), avg_duration_ref=200.0, reports_generated=1, warning_deviation_percent=150.0)
    assert decision.should_alert is True
    assert decision.alert_type == "task_failed"
    assert decision.semaphore.value == "red"


def test_rule_duration_deviation_warning() -> None:
    event = _base_event().model_copy(update={"duration": 400.0})
    decision = evaluate_event(event, DagContext(), avg_duration_ref=100.0, reports_generated=None, warning_deviation_percent=150.0)
    assert decision.should_alert is True
    assert decision.alert_type == "long_running"
    assert decision.semaphore.value == "yellow"


def test_rule_green_when_successful() -> None:
    event = _base_event().model_copy(update={"run_state": "success", "task_state": "success"})
    decision = evaluate_event(event, DagContext(expected_reports_count=1), avg_duration_ref=100.0, reports_generated=1, warning_deviation_percent=150.0)
    assert decision.should_alert is False
    assert decision.semaphore.value == "green"
