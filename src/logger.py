"""Observability: logging setup + run-level metrics tracker."""
from __future__ import annotations

import logging
import logging.handlers
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

from . import config

ROOT_LOGGER = "logpulse"
STATUS_SUCCESS = "SUCCESS"
STATUS_PARTIAL = "PARTIAL_FAILURE"
STATUS_FAILED = "FAILED"


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def new_run_id() -> str:
    return f"run_{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')}_{uuid.uuid4().hex[:8]}"


def get_logger(name: str) -> logging.Logger:
    return logging.getLogger(f"{ROOT_LOGGER}.{name}")


def configure_logging(level: int = logging.INFO, log_file: Optional[Path] = None, console: bool = True) -> None:
    """Attach console + rotating-file handlers to the 'logpulse' logger (idempotent)."""
    root = logging.getLogger(ROOT_LOGGER)
    root.setLevel(level)
    for h in list(root.handlers):
        root.removeHandler(h)
    fmt = logging.Formatter("%(asctime)s | %(levelname)-8s | %(name)s | %(message)s")
    if console:
        ch = logging.StreamHandler()
        ch.setFormatter(fmt)
        root.addHandler(ch)
    if log_file is None:
        log_file = config.LOG_DIR / "logpulse.log"
    try:
        Path(log_file).parent.mkdir(parents=True, exist_ok=True)
        fh = logging.handlers.RotatingFileHandler(log_file, maxBytes=2_000_000, backupCount=3, encoding="utf-8")
        fh.setFormatter(fmt)
        root.addHandler(fh)
    except OSError:
        root.warning("Could not open log file %s; logging to console only", log_file)
    root.propagate = False


logging.getLogger(ROOT_LOGGER).addHandler(logging.NullHandler())


@dataclass
class RunMetrics:
    """Everything we want to know about one pipeline execution."""
    run_id: str = field(default_factory=new_run_id)
    started_at: str = field(default_factory=utc_now_iso)
    ended_at: Optional[str] = None
    status: str = "RUNNING"
    failure_reason: Optional[str] = None

    files_total: int = 0
    files_processed: int = 0
    files_failed: int = 0
    files_skipped: int = 0
    blank_lines_skipped: int = 0

    input_records: int = 0
    parsed_records: int = 0
    parse_failures: int = 0
    validation_failures: int = 0
    conflict_rejections: int = 0
    duplicate_records: int = 0
    inserted_records: int = 0
    late_records: int = 0
    out_of_order_records: int = 0
    rejected_new: int = 0
    rejected_already_quarantined: int = 0

    rejected_by_reason: dict = field(default_factory=dict)
    file_results: list = field(default_factory=list)
    failures: list = field(default_factory=list)

    processing_seconds: float = 0.0
    duration_seconds: float = 0.0

    @property
    def rejected_records(self) -> int:
        return self.parse_failures + self.validation_failures + self.conflict_rejections

    @property
    def throughput_rps(self) -> float:
        return round(self.input_records / self.processing_seconds, 2) if self.processing_seconds > 0 else 0.0

    def add_failure(self, reason: str) -> None:
        self.failures.append(reason)
        self.failure_reason = "; ".join(self.failures)

    def to_dict(self) -> dict:
        d = {k: v for k, v in self.__dict__.items()}
        d["rejected_records"] = self.rejected_records
        d["throughput_records_per_sec"] = self.throughput_rps
        return d
