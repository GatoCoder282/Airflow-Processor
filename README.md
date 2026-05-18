# Airflow ETL Processor

Backend processor for Airflow monitoring events.

## Scope

- Consume `stream:airflow_events` and `stream:dag_catalog_sync` from Redis Streams.
- Enrich and persist data in PostgreSQL (`monitoring` schema).
- Publish processed alerts to `stream:alerts`.
- Expose operational endpoints via FastAPI.

## Run (dev)

1. Create `.env` from your environment values.
2. Install dependencies:

```bash
pip install -r requirements.txt
```

3. Run API + workers:

```bash
uvicorn processor.api.app:create_app --factory --host 0.0.0.0 --port 8000
```
