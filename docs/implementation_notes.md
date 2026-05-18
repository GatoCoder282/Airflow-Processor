# Implementation Notes

## Implemented in this iteration

- Hexagonal module layout under `processor/`.
- FastAPI app factory with lifespan startup/shutdown.
- Redis Streams consumer groups:
  - input: `stream:airflow_events`
  - input: `stream:dag_catalog_sync`
  - output: `stream:alerts`
- PostgreSQL async repository (`SQLAlchemy + asyncpg`) for:
  - `monitoring.dag_catalog` context read
  - `monitoring.dag_run_monitor` upsert
  - `monitoring.task_instance` insert
  - `monitoring.alert` insert
  - materialized view refresh jobs
- Rule engine for:
  - retry exhausted -> critical
  - duration deviation > threshold -> warning
  - reports generated < expected -> critical
- Anti false-positive mechanism:
  - critical pre-alert buffered in memory
  - grace window wait
  - evidence query before final emit
  - suppression audit via persisted info alert

## Pending integration checkpoints

- Confirm exact DDL for `monitoring.alert` optional columns.
- Confirm evidence source for report success if not in `monitoring.dag_run_monitor.reports_generated`.
- Add dispatch adapters for Slack/Telegram (currently only stream publication is active).
- Add integration tests with disposable Redis/Postgres.

## Runbook

1. Copy `.env.example` to `.env` and adjust values.
2. Install dependencies from `requirements.txt`.
3. Start service with uvicorn:

```bash
uvicorn processor.api.app:create_app --factory --host 0.0.0.0 --port 8000
```

4. Check health:

```bash
curl http://localhost:8000/health
curl http://localhost:8000/ready
```
