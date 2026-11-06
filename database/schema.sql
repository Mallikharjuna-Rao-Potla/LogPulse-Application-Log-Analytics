-- LogPulse SQLite schema.
-- Relationships:
--   services (1) <---- (N) processed_logs.service_name        [reference / referential integrity]
--   pipeline_runs (1) <-- (N) processed_logs.run_id, rejected_logs.run_id, processed_files.*_run_id
-- Derived fields (computed by the pipeline, never supplied by the source):
--   event_date, event_hour, is_error, is_late, payload_hash, event_id_generated

CREATE TABLE IF NOT EXISTS services (
    service_name TEXT PRIMARY KEY,
    description  TEXT
);

CREATE TABLE IF NOT EXISTS pipeline_runs (
    run_id               TEXT PRIMARY KEY,
    started_at           TEXT NOT NULL,
    ended_at             TEXT,
    status               TEXT NOT NULL CHECK (status IN ('RUNNING','SUCCESS','PARTIAL_FAILURE','FAILED')),
    failure_reason       TEXT,
    input_files          INTEGER NOT NULL DEFAULT 0,
    files_failed         INTEGER NOT NULL DEFAULT 0,
    input_records        INTEGER NOT NULL DEFAULT 0,
    inserted_records     INTEGER NOT NULL DEFAULT 0,
    duplicate_records    INTEGER NOT NULL DEFAULT 0,
    rejected_records     INTEGER NOT NULL DEFAULT 0,
    late_records         INTEGER NOT NULL DEFAULT 0,
    out_of_order_records INTEGER NOT NULL DEFAULT 0,
    processing_seconds   REAL,
    duration_seconds     REAL,
    throughput_rps       REAL
);

CREATE TABLE IF NOT EXISTS processed_files (
    file_id       INTEGER PRIMARY KEY AUTOINCREMENT,
    sha256        TEXT NOT NULL UNIQUE,              -- content checksum = file identity
    file_name     TEXT NOT NULL,
    file_path     TEXT NOT NULL,
    size_bytes    INTEGER NOT NULL,
    record_count  INTEGER NOT NULL DEFAULT 0,
    status        TEXT NOT NULL CHECK (status IN ('PROCESSED','FAILED')),
    last_error    TEXT,
    times_seen    INTEGER NOT NULL DEFAULT 1,
    first_run_id  TEXT NOT NULL REFERENCES pipeline_runs(run_id),
    last_run_id   TEXT NOT NULL REFERENCES pipeline_runs(run_id),
    first_seen_at TEXT NOT NULL,
    last_seen_at  TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS processed_logs (
    event_id           TEXT PRIMARY KEY,             -- idempotency key
    event_timestamp    TEXT NOT NULL,                -- ISO-8601 UTC, e.g. 2026-11-05T10:15:30Z
    event_date         TEXT NOT NULL,                -- derived: YYYY-MM-DD
    event_hour         TEXT NOT NULL,                -- derived: YYYY-MM-DDTHH:00:00Z
    severity           TEXT NOT NULL CHECK (severity IN ('INFO','WARNING','ERROR','CRITICAL')),
    service_name       TEXT NOT NULL REFERENCES services(service_name),
    message            TEXT NOT NULL,
    latency_ms         REAL CHECK (latency_ms IS NULL OR latency_ms >= 0),
    is_error           INTEGER NOT NULL CHECK (is_error IN (0,1)),   -- derived: ERROR/CRITICAL
    is_late            INTEGER NOT NULL DEFAULT 0 CHECK (is_late IN (0,1)),
    source_format      TEXT NOT NULL CHECK (source_format IN ('json','text')),
    event_id_generated INTEGER NOT NULL DEFAULT 0 CHECK (event_id_generated IN (0,1)),
    payload_hash       TEXT NOT NULL,                -- detects same-ID-different-content conflicts
    source_file        TEXT NOT NULL,
    source_line        INTEGER NOT NULL,
    file_sha256        TEXT NOT NULL,
    run_id             TEXT NOT NULL REFERENCES pipeline_runs(run_id),
    ingested_at        TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_logs_timestamp ON processed_logs(event_timestamp);
CREATE INDEX IF NOT EXISTS idx_logs_service   ON processed_logs(service_name);
CREATE INDEX IF NOT EXISTS idx_logs_severity  ON processed_logs(severity);
CREATE INDEX IF NOT EXISTS idx_logs_date      ON processed_logs(event_date);
CREATE INDEX IF NOT EXISTS idx_logs_run       ON processed_logs(run_id);

CREATE TABLE IF NOT EXISTS rejected_logs (
    reject_id         INTEGER PRIMARY KEY AUTOINCREMENT,
    reject_key        TEXT NOT NULL UNIQUE,          -- sha1(file checksum | line | raw): makes quarantine idempotent
    run_id            TEXT NOT NULL REFERENCES pipeline_runs(run_id),
    stage             TEXT NOT NULL CHECK (stage IN ('PARSING','VALIDATION')),
    primary_reason    TEXT NOT NULL,
    rejection_reasons TEXT NOT NULL,                 -- ';'-separated reason codes
    event_id          TEXT,
    source_format     TEXT,
    source_file       TEXT NOT NULL,
    source_line       INTEGER NOT NULL,
    file_sha256       TEXT NOT NULL,
    raw_record        TEXT NOT NULL,                 -- original line, never altered
    rejected_at       TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_rejected_reason ON rejected_logs(primary_reason);
CREATE INDEX IF NOT EXISTS idx_rejected_run    ON rejected_logs(run_id);
