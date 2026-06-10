import os

from pydantic_settings import BaseSettings, SettingsConfigDict


class BackendConfig(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8", extra="ignore")

    redis_host: str = "localhost"
    redis_port: int = 6379
    redis_password: str | None = None
    redis_db: int = 0

    postgres_dsn: str = "postgresql://datax_backend:change_me@localhost:5432/datax"
    pg_pool_min: int = 2
    pg_pool_max: int = 10

    # Base operacional platform_db (solo lectura, opcional). Vacío = enriquecimiento deshabilitado.
    platform_db_dsn: str = ""

    telegram_bot_token: str | None = None
    telegram_chat_id: str | None = None
    taiga_url: str | None = None
    taiga_auth_token: str | None = None
    taiga_project_id: int | None = None

    region: str = "BO"
    api_host: str = "0.0.0.0"
    api_port: int = 8000

    stream_airflow_events: str = "stream:airflow_events"
    stream_alerts: str = "stream:alerts"
    stream_catalog_sync: str = "stream:dag_catalog_sync"

    group_backend_processors: str = "backend_processors"
    group_catalog_sync: str = "catalog_sync_processors"
    consumer_name: str = "backend_worker_1"

    duration_deviation_warning_percent: float = 150.0
    refresh_current_status_seconds: int = 15
    refresh_performance_seconds: int = 300
    refresh_broken_url_seconds: int = 600
    refresh_broken_url_priority_seconds: int = 600
    app_name: str = "ETL Observability Backend"
    app_log_level: str = "INFO"

    @classmethod
    def from_env(cls) -> "BackendConfig":
        return cls(
            redis_host=os.getenv("REDIS_HOST", "localhost"),
            redis_port=int(os.getenv("REDIS_PORT", "6379")),
            redis_password=os.getenv("REDIS_PASSWORD") or None,
            redis_db=int(os.getenv("REDIS_DB", "0")),
            postgres_dsn=os.getenv("DATABASE_URL", "postgresql://datax_backend:change_me@localhost:5432/datax"),
            pg_pool_min=int(os.getenv("PG_POOL_MIN", "2")),
            pg_pool_max=int(os.getenv("PG_POOL_MAX", "10")),
            platform_db_dsn=os.getenv("PLATFORM_DB_DSN", ""),
            telegram_bot_token=os.getenv("TELEGRAM_BOT_TOKEN") or None,
            telegram_chat_id=os.getenv("TELEGRAM_CHAT_ID") or None,
            taiga_url=os.getenv("TAIGA_URL") or None,
            taiga_auth_token=os.getenv("TAIGA_AUTH_TOKEN") or None,
            taiga_project_id=int(os.getenv("TAIGA_PROJECT_ID")) if os.getenv("TAIGA_PROJECT_ID") else None,
            region=os.getenv("REGION", "BO"),
            api_host=os.getenv("API_HOST", "0.0.0.0"),
            api_port=int(os.getenv("API_PORT", "8000")),
            stream_airflow_events=os.getenv("STREAM_AIRFLOW_EVENTS", "stream:airflow_events"),
            stream_alerts=os.getenv("STREAM_ALERTS", "stream:alerts"),
            stream_catalog_sync=os.getenv("STREAM_CATALOG_SYNC", "stream:dag_catalog_sync"),
            group_backend_processors=os.getenv("GROUP_BACKEND_PROCESSORS", "backend_processors"),
            group_catalog_sync=os.getenv("GROUP_CATALOG_SYNC", "catalog_sync_processors"),
            consumer_name=os.getenv("CONSUMER_NAME", "backend_worker_1"),
            duration_deviation_warning_percent=float(os.getenv("DURATION_DEVIATION_WARNING_PERCENT", "150.0")),
            refresh_current_status_seconds=int(os.getenv("REFRESH_CURRENT_STATUS_SECONDS", "15")),
            refresh_performance_seconds=int(os.getenv("REFRESH_PERFORMANCE_SECONDS", "300")),
            refresh_broken_url_seconds=int(os.getenv("REFRESH_BROKEN_URL_SECONDS", "600")),
            refresh_broken_url_priority_seconds=int(os.getenv("REFRESH_BROKEN_URL_PRIORITY_SECONDS", "600")),
            app_name=os.getenv("APP_NAME", "ETL Observability Backend"),
            app_log_level=os.getenv("APP_LOG_LEVEL", "INFO"),
        )


ProcessorSettings = BackendConfig
