"""Central configuration for LogPulse. Every tunable lives here."""
from __future__ import annotations

import os
from datetime import datetime, timezone
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
DATA_DIR = PROJECT_ROOT / "data"
SAMPLE_DIR = DATA_DIR / "sample"
GENERATED_DIR = DATA_DIR / "generated"
OUTPUT_DIR = DATA_DIR / "outputs"
LOG_DIR = PROJECT_ROOT / "logs"
SCHEMA_PATH = PROJECT_ROOT / "database" / "schema.sql"
DB_PATH = Path(os.environ.get("LOGPULSE_DB", PROJECT_ROOT / "database" / "logpulse.db"))

# Reproducibility
SEED = 42

# Severity model
VALID_SEVERITIES = ("INFO", "WARNING", "ERROR", "CRITICAL")
ERROR_SEVERITIES = ("ERROR", "CRITICAL")
SEVERITY_ALIASES = {
    "info": "INFO", "information": "INFO",
    "warn": "WARNING", "warning": "WARNING",
    "err": "ERROR", "error": "ERROR",
    "crit": "CRITICAL", "critical": "CRITICAL",
}

# Reference entity (referential integrity): services we accept logs from.
KNOWN_SERVICES = {
    "auth": "Authentication and session management",
    "payment": "Payment processing",
    "inventory": "Stock and warehouse management",
    "orders": "Order management",
    "notifications": "Email and push notifications",
    "search": "Search and indexing",
    "gateway": "API gateway",
}

# Column-name standardisation: any key on the left is mapped to the standard name.
FIELD_ALIASES = {
    "event_id": "event_id", "eventid": "event_id", "id": "event_id",
    "timestamp": "timestamp", "time": "timestamp", "ts": "timestamp", "datetime": "timestamp",
    "severity": "severity", "level": "severity", "log_level": "severity", "loglevel": "severity",
    "service_name": "service_name", "service": "service_name", "svc": "service_name",
    "message": "message", "msg": "message",
    "latency_ms": "latency_ms", "latency": "latency_ms",
}
STANDARD_FIELDS = ("event_id", "timestamp", "severity", "service_name", "message", "latency_ms")
REQUIRED_FIELDS = ("event_id", "timestamp", "severity", "service_name", "message")  # latency_ms optional

# Strings that are standardised to NULL.
MISSING_TOKENS = {"", "null", "none", "nan", "n/a", "na", "-"}

# Validation / business rules
EVENT_ID_PATTERN = r"^[A-Za-z0-9][A-Za-z0-9_.:\-]{0,63}$"
MAX_LATENCY_MS = 600_000.0  # 10 minutes: anything above is treated as a bad measurement
MIN_TIMESTAMP_EPOCH = datetime(2000, 1, 1, tzinfo=timezone.utc).timestamp()
MAX_TIMESTAMP_EPOCH = datetime(2100, 1, 1, tzinfo=timezone.utc).timestamp()

# Late events: older than the watermark by more than this many minutes.
LATE_THRESHOLD_MINUTES = 30

# Performance: fast paths in normalisation/validation (identical results, ~30% faster).
# Set LOGPULSE_NO_FASTPATH=1 to disable (used by the benchmark to measure the optimisation).
FAST_PATHS = os.environ.get("LOGPULSE_NO_FASTPATH", "0") != "1"

# Storage
DB_BATCH_SIZE = 1000
DB_LOOKUP_CHUNK = 500
DB_RETRY_ATTEMPTS = 3
DB_RETRY_DELAY_SECONDS = 0.25

# Input discovery
SUPPORTED_EXTENSIONS = {".jsonl", ".ndjson", ".txt", ".log"}
