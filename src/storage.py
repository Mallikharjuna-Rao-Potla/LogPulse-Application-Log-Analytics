"""SQLite storage layer: schema, transactions, idempotent inserts, run/file tracking."""
from __future__ import annotations

import sqlite3
from contextlib import contextmanager
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Tuple

from . import config
from .logger import get_logger, utc_now_iso

log = get_logger("storage")

PROCESSED_COLUMNS = (
    "event_id", "event_timestamp", "event_date", "event_hour", "severity", "service_name", "message",
    "latency_ms", "is_error", "is_late", "source_format", "event_id_generated", "payload_hash",
    "source_file", "source_line", "file_sha256", "run_id", "ingested_at",
)
REJECTED_COLUMNS = (
    "reject_key", "run_id", "stage", "primary_reason", "rejection_reasons", "event_id", "source_format",
    "source_file", "source_line", "file_sha256", "raw_record", "rejected_at",
)


def is_transient(exc: Exception) -> bool:
    """Locked/busy databases are worth retrying; constraint or schema errors are not."""
    msg = str(exc).lower()
    return isinstance(exc, sqlite3.OperationalError) and ("locked" in msg or "busy" in msg)


class Storage:
    def __init__(self, db_path=config.DB_PATH, schema_path=config.SCHEMA_PATH):
        self.db_path = str(db_path)
        self.schema_path = Path(schema_path)
        if self.db_path != ":memory:":
            Path(self.db_path).parent.mkdir(parents=True, exist_ok=True)
        # isolation_level=None -> we control transactions explicitly
        self.conn = sqlite3.connect(self.db_path, timeout=30, isolation_level=None)
        self.conn.row_factory = sqlite3.Row
        self.conn.execute("PRAGMA foreign_keys = ON")
        if self.db_path != ":memory:":
            self.conn.execute("PRAGMA journal_mode = WAL")
            self.conn.execute("PRAGMA synchronous = NORMAL")
        self._in_txn = False
        self.init_schema()

    # ---- lifecycle --------------------------------------------------------------------
    def init_schema(self) -> None:
        self.conn.executescript(self.schema_path.read_text(encoding="utf-8"))
        self.conn.executemany("INSERT OR IGNORE INTO services(service_name, description) VALUES (?, ?)",
                              list(config.KNOWN_SERVICES.items()))

    def close(self) -> None:
        self.conn.close()

    @contextmanager
    def transaction(self):
        """Atomic unit of work: COMMIT on success, ROLLBACK on any exception (re-raised)."""
        if self._in_txn:          # nested use joins the outer transaction
            yield
            return
        self.conn.execute("BEGIN IMMEDIATE")
        self._in_txn = True
        try:
            yield
        except BaseException:
            self.conn.execute("ROLLBACK")
            raise
        else:
            self.conn.execute("COMMIT")
        finally:
            self._in_txn = False

    # ---- reference data ---------------------------------------------------------------
    def get_known_services(self) -> set:
        return {r[0] for r in self.conn.execute("SELECT service_name FROM services")}

    # ---- pipeline runs ----------------------------------------------------------------
    def start_run(self, run_id: str, started_at: str) -> None:
        with self.transaction():
            self.conn.execute("INSERT INTO pipeline_runs(run_id, started_at, status) VALUES (?, ?, 'RUNNING')",
                              (run_id, started_at))

    def finish_run(self, m) -> None:
        with self.transaction():
            self.conn.execute(
                """UPDATE pipeline_runs SET ended_at=?, status=?, failure_reason=?, input_files=?, files_failed=?,
                          input_records=?, inserted_records=?, duplicate_records=?, rejected_records=?,
                          late_records=?, out_of_order_records=?, processing_seconds=?, duration_seconds=?,
                          throughput_rps=? WHERE run_id=?""",
                (m.ended_at, m.status, m.failure_reason, m.files_total, m.files_failed, m.input_records,
                 m.inserted_records, m.duplicate_records, m.rejected_records, m.late_records,
                 m.out_of_order_records, round(m.processing_seconds, 4), round(m.duration_seconds, 4),
                 m.throughput_rps, m.run_id))

    # ---- processed files --------------------------------------------------------------
    def get_processed_file(self, sha256: str) -> Optional[sqlite3.Row]:
        return self.conn.execute("SELECT * FROM processed_files WHERE sha256=?", (sha256,)).fetchone()

    def upsert_processed_file(self, *, sha256, file_name, file_path, size_bytes, record_count,
                              status, run_id, last_error=None) -> None:
        now = utc_now_iso()
        self.conn.execute(
            """INSERT INTO processed_files(sha256, file_name, file_path, size_bytes, record_count, status,
                      last_error, times_seen, first_run_id, last_run_id, first_seen_at, last_seen_at)
               VALUES (?,?,?,?,?,?,?,1,?,?,?,?)
               ON CONFLICT(sha256) DO UPDATE SET
                   file_name=excluded.file_name, file_path=excluded.file_path, size_bytes=excluded.size_bytes,
                   record_count=excluded.record_count, status=excluded.status, last_error=excluded.last_error,
                   times_seen=processed_files.times_seen+1, last_run_id=excluded.last_run_id,
                   last_seen_at=excluded.last_seen_at""",
            (sha256, file_name, file_path, size_bytes, record_count, status, last_error,
             run_id, run_id, now, now))

    # ---- duplicate lookup -------------------------------------------------------------
    def fetch_existing_hashes(self, event_ids: List[str], mode: str = "batch") -> Dict[str, str]:
        """event_id -> payload_hash for IDs already stored. ``batch`` = chunked IN(...) lookups."""
        found: Dict[str, str] = {}
        if not event_ids:
            return found
        if mode == "row":   # naive baseline (one query per record) - used by the benchmark
            for eid in event_ids:
                row = self.conn.execute("SELECT payload_hash FROM processed_logs WHERE event_id=?", (eid,)).fetchone()
                if row:
                    found[eid] = row[0]
            return found
        step = config.DB_LOOKUP_CHUNK
        for i in range(0, len(event_ids), step):
            chunk = event_ids[i:i + step]
            marks = ",".join("?" * len(chunk))
            for row in self.conn.execute(
                    f"SELECT event_id, payload_hash FROM processed_logs WHERE event_id IN ({marks})", chunk):
                found[row[0]] = row[1]
        return found

    # ---- inserts ----------------------------------------------------------------------
    def insert_processed_logs(self, records: Iterable[dict], run_id: str, mode: str = "batch") -> int:
        """Insert with ``INSERT OR IGNORE`` (event_id PK) and return the number really inserted."""
        now = utc_now_iso()
        rows = [tuple((r[c] if c not in ("run_id", "ingested_at") else (run_id if c == "run_id" else now))
                      for c in PROCESSED_COLUMNS) for r in records]
        sql = (f"INSERT OR IGNORE INTO processed_logs ({','.join(PROCESSED_COLUMNS)}) "
               f"VALUES ({','.join('?' * len(PROCESSED_COLUMNS))})")
        before = self.conn.total_changes
        if mode == "row":
            for row in rows:
                self.conn.execute(sql, row)
        else:
            step = config.DB_BATCH_SIZE
            for i in range(0, len(rows), step):
                self.conn.executemany(sql, rows[i:i + step])
        return self.conn.total_changes - before

    def insert_rejected_logs(self, records: Iterable[dict]) -> Tuple[int, int]:
        """Quarantine rows. Returns (newly_inserted, already_quarantined)."""
        rows = [tuple(r[c] for c in REJECTED_COLUMNS) for r in records]
        if not rows:
            return 0, 0
        sql = (f"INSERT OR IGNORE INTO rejected_logs ({','.join(REJECTED_COLUMNS)}) "
               f"VALUES ({','.join('?' * len(REJECTED_COLUMNS))})")
        before = self.conn.total_changes
        step = config.DB_BATCH_SIZE
        for i in range(0, len(rows), step):
            self.conn.executemany(sql, rows[i:i + step])
        new = self.conn.total_changes - before
        return new, len(rows) - new

    # ---- reads ------------------------------------------------------------------------
    def max_event_epoch(self) -> Optional[float]:
        """Watermark: newest event time already in the warehouse (epoch seconds)."""
        row = self.conn.execute("SELECT MAX(event_timestamp) FROM processed_logs").fetchone()
        if not row or row[0] is None:
            return None
        from .transformation import parse_timestamp
        return parse_timestamp(row[0]).timestamp()

    def count(self, table: str) -> int:
        if table not in ("processed_logs", "rejected_logs", "pipeline_runs", "processed_files", "services"):
            raise ValueError(f"unknown table {table}")
        return self.conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
