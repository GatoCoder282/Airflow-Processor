from enum import Enum


class EventType(str, Enum):
    TASK_STATE_CHANGE = "task_state_change"
    DAG_RUN_STATE_CHANGE = "dag_run_state_change"
    TASK_LOG = "task_log"
    DAG_WARNING = "dag_warning"
    IMPORT_ERROR = "import_error"
    IMPORT_ERROR_DETECTED = "import_error_detected"
    SCHEDULER_UNHEALTHY = "scheduler_unhealthy"
    DAG_CATALOG_SYNC = "dag_catalog_sync"


class AlertType(str, Enum):
    TASK_FAILED = "task_failed"
    SLA_BREACH = "sla_breach"
    LONG_RUNNING = "long_running"
    IMPORT_ERROR = "import_error"
    SCHEDULER_DOWN = "scheduler_down"
    RETRY_EXCEEDED = "retry_exceeded"
    REPORT_NOT_GENERATED = "report_not_generated"
    URL_DEAD = "url_dead"
    DOWNLOAD_DELAY = "download_delay"


class AlertSeverity(str, Enum):
    CRITICAL = "critical"
    WARNING = "warning"
    INFO = "info"


class IncidentCategory(str, Enum):
    URL_DEAD = "url_dead"
    REPORT_NOT_GENERATED = "report_not_generated"
    DOWNLOAD_DELAY = "download_delay"
    RETRY_EXCEEDED = "retry_exceeded"
    STRUCTURE_CHANGE = "structure_change"
    SCHEDULER_ISSUE = "scheduler_issue"
    IMPORT_ERROR = "import_error"
    MISSING_REPORTS = "missing_reports"


class SemaphoreColor(str, Enum):
    GREEN = "green"
    YELLOW = "yellow"
    RED = "red"


class NotificationChannel(str, Enum):
    SLACK = "slack"
    TELEGRAM = "telegram"
    EMAIL = "email"


Severity = AlertSeverity
Semaphore = SemaphoreColor
