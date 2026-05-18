from __future__ import annotations

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from dataclasses import dataclass, field

from fastapi.testclient import TestClient

import processor.api.app as app_module


@dataclass
class FakeRedis:
    ping_called: bool = False
    closed: bool = False

    async def ping(self) -> bool:
        self.ping_called = True
        return True

    async def aclose(self) -> None:
        self.closed = True


@dataclass
class FakeConnection:
    rows_by_query: dict[str, list[dict[str, object]]]
    executed: list[tuple[str, tuple[object, ...]]] = field(default_factory=list)

    async def fetch(self, query: str, *params: object) -> list[dict[str, object]]:
        normalized = " ".join(query.split())
        self.executed.append((normalized, params))
        if "monitoring.alert" in normalized:
            rows = list(self.rows_by_query.get("monitoring.alert", []))
            param_index = 0
            if "resolved =" in normalized:
                resolved = params[param_index]
                rows = [row for row in rows if row.get("resolved") == resolved]
                param_index += 1
            if "severity =" in normalized:
                severity = params[param_index]
                rows = [row for row in rows if row.get("severity") == severity]
                param_index += 1
            if "dag_id =" in normalized:
                dag_id = params[param_index]
                rows = [row for row in rows if row.get("dag_id") == dag_id]
            return rows
        if "monitoring.dag_current_status" in normalized:
            rows = list(self.rows_by_query.get("monitoring.dag_current_status", []))
            param_index = 0
            if "region =" in normalized:
                region = params[param_index]
                rows = [row for row in rows if row.get("region", "BO") == region]
                param_index += 1
            if "last_semaphore =" in normalized:
                semaphore = params[param_index]
                rows = [row for row in rows if row.get("last_semaphore") == semaphore]
                param_index += 1
            if "criticality =" in normalized:
                criticality = params[param_index]
                rows = [row for row in rows if row.get("criticality") == criticality]
                param_index += 1
            if "dag_type =" in normalized:
                dag_type = params[param_index]
                rows = [row for row in rows if row.get("dag_type") == dag_type]
            return rows
        if "monitoring.report_incidence" in normalized:
            rows = list(self.rows_by_query.get("monitoring.report_incidence", []))
            param_index = 1  # region is always first param in this endpoint
            if "status =" in normalized:
                status = params[param_index]
                rows = [row for row in rows if row.get("status") == status]
                param_index += 1
            if "category =" in normalized:
                category = params[param_index]
                rows = [row for row in rows if row.get("category") == category]
                param_index += 1
            if "dag_id =" in normalized:
                dag_id = params[param_index]
                rows = [row for row in rows if row.get("dag_id") == dag_id]

            return sorted(
                rows,
                key=lambda row: (float(row.get("priority_score", 0)), str(row.get("detected_at", ""))),
                reverse=True,
            )
        if "monitoring.incidence_timeline" in normalized:
            rows = list(self.rows_by_query.get("monitoring.incidence_timeline", []))
            if params:
                region = params[0]
                rows = [row for row in rows if row.get("region") == region]
            return rows
        if "monitoring.broken_url_priority" in normalized:
            rows = list(self.rows_by_query.get("monitoring.broken_url_priority", []))
            if "category =" in normalized and params:
                rows = [row for row in rows if row.get("category") == params[0]]
            return rows
        for key, rows in self.rows_by_query.items():
            if key in normalized:
                return rows
        return []

    async def fetchrow(self, query: str, *params: object):
        normalized = " ".join(query.split())
        self.executed.append((normalized, params))
        if "FROM monitoring.kpi_summary" in normalized and "WHERE region =" in normalized:
            rows = self.rows_by_query.get("monitoring.kpi_summary", [])
            region = params[0] if params else None
            for row in rows:
                if row.get("region") == region:
                    return row
            return None
        if "FROM monitoring.task_instance" in normalized and "LIMIT 1" in normalized:
            rows = self.rows_by_query.get("monitoring.task_instance", [])
            for row in rows:
                if row.get("state") == "failed":
                    return row
            return rows[0] if rows else None
        if "FROM monitoring.dag_report_link" in normalized and "LIMIT 1" in normalized:
            rows = self.rows_by_query.get("monitoring.dag_report_link", [])
            return rows[0] if rows else None
        rows = self.rows_by_query.get(next((key for key in self.rows_by_query if key in normalized), ""), [])
        return rows[0] if rows else None

    async def fetchval(self, query: str, *params: object):
        normalized = " ".join(query.split())
        self.executed.append((normalized, params))
        if normalized.strip().upper() == "SELECT 1":
            return 1
        if "FROM monitoring.report_run_expectation" in normalized:
            rows = self.rows_by_query.get("monitoring.report_run_expectation", [])
            region = params[0] if params else None
            filtered = [row for row in rows if row.get("region") == region]
            if "missing_reports_count > 0" in normalized:
                filtered = [row for row in filtered if int(row.get("missing_reports_count", 0)) > 0]
            return len(filtered)
        if "FROM monitoring.broken_url_summary" in normalized:
            rows = self.rows_by_query.get("monitoring.broken_url_summary", [])
            return len(rows)
        return None

    async def execute(self, query: str, *params: object) -> str:
        normalized = " ".join(query.split())
        self.executed.append((normalized, params))
        return "OK"

    async def __aenter__(self) -> "FakeConnection":
        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:
        return None


@dataclass
class FakePool:
    rows_by_query: dict[str, list[dict[str, object]]]
    acquired: list[FakeConnection] = field(default_factory=list)

    def acquire(self) -> FakeConnection:
        connection = FakeConnection(self.rows_by_query)
        self.acquired.append(connection)
        return connection

    async def close(self) -> None:
        return None


class FakeWorker:
    def __init__(self) -> None:
        self.ran = False

    async def run(self) -> None:
        self.ran = True


class FakeFactory:
    def __init__(self) -> None:
        self.config = type("Config", (), {"app_name": "ETL Observability Backend"})()
        self.redis_client = FakeRedis()
        self.db_pool = FakePool(
            {
                "monitoring.dag_current_status": [
                    {"dag_id": "dag_1", "last_semaphore": "green", "criticality": "medium", "dag_type": "C"},
                    {"dag_id": "dag_2", "last_semaphore": "red", "criticality": "high", "dag_type": "D"},
                ],
                "monitoring.dag_run_monitor": [
                    {"dag_id": "dag_1", "run_id": "run_1", "state": "success", "region": "BO"},
                ],
                "monitoring.alert": [
                    {"id": 1, "dag_id": "dag_1", "severity": "warning", "resolved": False},
                    {"id": 2, "dag_id": "dag_2", "severity": "critical", "resolved": True},
                ],
                "monitoring.task_instance": [
                    {
                        "task_id": "extract",
                        "state": "failed",
                        "upstream_task_id": None,
                        "log_excerpt": "traceback",
                        "last_log_token": "tok-1",
                        "updated_at": "2026-04-09T10:00:00+00:00",
                        "downstream_task_ids": ["transform"],
                    },
                    {
                        "task_id": "transform",
                        "state": "upstream_failed",
                        "upstream_task_id": "extract",
                        "log_excerpt": None,
                        "last_log_token": None,
                        "updated_at": "2026-04-09T10:01:00+00:00",
                        "downstream_task_ids": ["load"],
                    },
                ],
                "monitoring.dag_report_link": [
                    {"id_report": 777, "id_file": 888},
                ],
                "monitoring.report_incidence": [
                    {
                        "id": 1,
                        "region": "BO",
                        "dag_id": "dag_1",
                        "status": "open",
                        "category": "report_not_generated",
                        "priority_score": 95.0,
                        "detected_at": "2026-04-09T08:00:00+00:00",
                    },
                    {
                        "id": 2,
                        "region": "BO",
                        "dag_id": "dag_2",
                        "status": "open",
                        "category": "download_delay",
                        "priority_score": 70.0,
                        "detected_at": "2026-04-09T09:00:00+00:00",
                    },
                ],
                "monitoring.kpi_summary": [
                    {
                        "region": "BO",
                        "failed_runs_today": 1,
                        "total_runs_today": 4,
                        "failure_rate_today_pct": 25.0,
                        "failed_runs_week": 2,
                        "failure_rate_week_pct": 20.0,
                        "runs_with_no_reports_today": 1,
                        "open_alerts": 3,
                        "open_critical_alerts": 1,
                    },
                ],
                "monitoring.report_run_expectation": [
                    {"region": "BO", "missing_reports_count": 2},
                    {"region": "BO", "missing_reports_count": 0},
                ],
                "monitoring.broken_url_summary": [
                    {"url": "https://broken-1"},
                    {"url": "https://broken-2"},
                ],
                "monitoring.incidence_timeline": [
                    {
                        "period": "2026-04-09",
                        "region": "BO",
                        "category": "report_not_generated",
                        "severity": "critical",
                        "total_incidences": 5,
                        "resolved_count": 2,
                        "open_count": 3,
                        "avg_resolution_seconds": 123.0,
                        "max_priority_score": 95.0,
                    },
                ],
                "monitoring.broken_url_priority": [
                    {
                        "priority_score": 95.0,
                        "url_fail_count": 7,
                        "category": "url_dead",
                        "url": "https://broken-1",
                        "report_name": "Report A",
                        "report_code": "R-A",
                        "source_name": "Source A",
                        "last_failure_date": "2026-04-09",
                        "first_seen": "2026-04-01",
                        "last_seen": "2026-04-09",
                        "dag_criticality": "high",
                    },
                ],
            }
        )
        self.event_processor = FakeWorker()
        self.event_processor.get_metrics = lambda: {
            "processed_events": 3,
            "discarded_events": 0,
            "alerts_created": 1,
            "alerts_deduplicated": 1,
            "alerts_suppressed": 0,
        }
        self.event_processor._alert_queue = type("QueueLike", (), {"qsize": lambda self: 0})()
        self.event_processor._consumer = type("ConsumerLike", (), {"get_metrics": lambda self: {"consumed_events": 3, "parse_errors": 0}})()
        self.alert_dispatcher = FakeWorker()
        self.alert_dispatcher.get_metrics = lambda: {"alerts_sent": 1, "alerts_failed": 0, "dispatch_timeouts": 0}
        self.view_scheduler = FakeWorker()
        self.view_scheduler.get_metrics = lambda: {
            "current_status": {
                "success_count": 1,
                "error_count": 0,
                "last_duration_ms": 10.0,
                "last_error": None,
                "last_run_at": 1,
            }
        }
        self.catalog_sync_worker = FakeWorker()

    async def close(self) -> None:
        await self.redis_client.aclose()
        await self.db_pool.close()


async def _fake_create(_: object) -> FakeFactory:
    return FakeFactory()


def _build_client(monkeypatch) -> TestClient:
    monkeypatch.setattr(app_module.BackendConfig, "from_env", classmethod(lambda cls: cls(app_name="ETL Observability Backend")))
    monkeypatch.setattr(app_module.BackendFactory, "create", _fake_create)
    monkeypatch.setattr(app_module, "configure_logging", lambda *_args, **_kwargs: None)
    app = app_module.create_app()
    return TestClient(app)


def test_health_and_ready_endpoints(monkeypatch) -> None:
    with _build_client(monkeypatch) as client:
        response = client.get("/health")
        assert response.status_code == 200
        payload = response.json()
        assert payload["status"] == "ok"
        assert payload["service"] == "ETL Observability Backend"
        assert payload["components"]["event_processor"] == "running"
        assert "consumer" in payload["metrics"]

        ready_response = client.get("/ready")
        assert ready_response.status_code == 200
        ready_payload = ready_response.json()
        assert ready_payload["status"] == "ready"
        assert ready_payload["components"]["redis"]["status"] == "ready"
        assert ready_payload["components"]["postgres"]["status"] == "ready"

        factory = client.app.state.factory
        assert factory.redis_client.ping_called is True


def test_dags_endpoints_return_rows(monkeypatch) -> None:
    with _build_client(monkeypatch) as client:
        response = client.get("/dags", params={"region": "BO", "semaphore": "green", "limit": 10, "offset": 0})
        assert response.status_code == 200
        assert response.json() == [
            {"dag_id": "dag_1", "last_semaphore": "green", "criticality": "medium", "dag_type": "C"},
        ]

        detail_response = client.get("/dags/dag_1", params={"region": "BO"})
        assert detail_response.status_code == 200
        assert detail_response.json() == [{"dag_id": "dag_1", "run_id": "run_1", "state": "success", "region": "BO"}]


def test_alerts_endpoints_update_records(monkeypatch) -> None:
    with _build_client(monkeypatch) as client:
        list_response = client.get("/alerts", params={"resolved": False, "severity": "warning", "dag_id": "dag_1"})
        assert list_response.status_code == 200
        assert list_response.json() == [{"id": 1, "dag_id": "dag_1", "severity": "warning", "resolved": False}]

        resolve_response = client.patch("/alerts/1/resolve")
        assert resolve_response.status_code == 200
        assert resolve_response.json() == {"status": "ok", "alert_id": 1, "resolved": True}

        acknowledge_response = client.patch("/alerts/2/acknowledge", json={"acknowledged_by": "tech-01"})
        assert acknowledge_response.status_code == 200
        assert acknowledge_response.json() == {"status": "ok", "alert_id": 2, "acknowledged": True}

        suppress_response = client.patch(
            "/alerts/2/suppress",
            json={"suppressed": True, "suppression_reason": "maintenance"},
        )
        assert suppress_response.status_code == 200
        assert suppress_response.json() == {
            "status": "ok",
            "alert_id": 2,
            "suppressed": True,
            "suppression_reason": "maintenance",
        }

        executed_queries = [
            query
            for connection in client.app.state.factory.db_pool.acquired
            for query, _ in connection.executed
        ]
        assert any("UPDATE monitoring.alert SET acknowledged = TRUE" in query for query in executed_queries)
        assert any("resolved = TRUE" in query and "updated_at = NOW()" in query for query in executed_queries)
        assert any("acknowledged = TRUE" in query and "updated_at = NOW()" in query for query in executed_queries)
        assert any("suppressed = $2" in query and "updated_at = NOW()" in query for query in executed_queries)


def test_kpis_endpoint_returns_summary(monkeypatch) -> None:
    with _build_client(monkeypatch) as client:
        response = client.get("/kpis")
        assert response.status_code == 200
        assert response.json()[0]["region"] == "BO"


def test_incidences_endpoint_returns_prioritized_rows(monkeypatch) -> None:
    with _build_client(monkeypatch) as client:
        response = client.get("/incidences", params={"region": "BO", "status": "open"})
        assert response.status_code == 200
        payload = response.json()
        assert len(payload) == 2
        assert payload[0]["priority_score"] == 95.0
        assert payload[1]["priority_score"] == 70.0


def test_dag_run_root_cause_endpoint_resolves_failed_task(monkeypatch) -> None:
    with _build_client(monkeypatch) as client:
        response = client.get("/dags/dag_1/runs/run_1/root-cause", params={"region": "BO"})
        assert response.status_code == 200
        payload = response.json()
        assert payload["root_cause"]["task_id"] == "extract"
        assert payload["root_cause"]["state"] == "failed"
        assert payload["affected_report"] == {"id_report": 777, "id_file": 888}
        assert len(payload["tasks"]) == 2


def test_update_incidence_status_endpoint(monkeypatch) -> None:
    with _build_client(monkeypatch) as client:
        response = client.patch(
            "/incidences/1/status",
            json={"status": "in_progress", "observations": "assigned"},
        )
        assert response.status_code == 200
        assert response.json() == {"status": "ok", "incidence_id": 1, "new_status": "in_progress"}

        invalid = client.patch("/incidences/1/status", json={"status": "invalid"})
        assert invalid.status_code == 200
        assert invalid.json()["status"] == "error"


def test_kpis_extended_endpoint(monkeypatch) -> None:
    with _build_client(monkeypatch) as client:
        response = client.get("/kpis/extended", params={"region": "BO"})
        assert response.status_code == 200
        payload = response.json()
        assert payload["region"] == "BO"
        assert payload["daily"]["failure_rate_pct"] == 25.0
        assert payload["reporting"]["missing_reports_today"] == 1
        assert payload["urls"]["dead_urls_30d"] == 2


def test_incidences_timeline_endpoint(monkeypatch) -> None:
    with _build_client(monkeypatch) as client:
        response = client.get("/incidences/timeline", params={"region": "BO", "granularity": "day"})
        assert response.status_code == 200
        payload = response.json()
        assert payload["granularity"] == "day"
        assert len(payload["rows"]) == 1


def test_urls_prioritized_and_excel_export(monkeypatch) -> None:
    with _build_client(monkeypatch) as client:
        list_response = client.get("/urls/prioritized")
        assert list_response.status_code == 200
        assert list_response.json()[0]["priority_score"] == 95.0

        export_response = client.get("/urls/prioritized/export.xlsx")
        assert export_response.status_code == 200
        assert export_response.headers["content-type"].startswith(
            "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        )
        assert export_response.content[:2] == b"PK"


def test_refresh_metrics_endpoint(monkeypatch) -> None:
    with _build_client(monkeypatch) as client:
        response = client.get("/health/refresh-metrics")
        assert response.status_code == 200
        payload = response.json()
        assert payload["status"] == "ok"
        assert "current_status" in payload["metrics"]
