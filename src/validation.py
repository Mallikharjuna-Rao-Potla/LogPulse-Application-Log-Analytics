"""Validation layer: rule checks, duplicate detection and the quarantine record builder.

Invalid records are NEVER deleted: they get a list of rejection reason codes and are
written to ``rejected_logs`` together with their untouched raw line.
"""
from __future__ import annotations

import hashlib
import re
from typing import Dict, Iterable, List, Optional, Set, Tuple

from . import config
from .logger import utc_now_iso

# ---- Reason codes (documented in docs/data_dictionary.md) -------------------------------
PARSE_ERROR_MALFORMED_JSON = "PARSE_ERROR_MALFORMED_JSON"
PARSE_ERROR_UNEXPECTED_FORMAT = "PARSE_ERROR_UNEXPECTED_FORMAT"
UNEXPECTED_SCHEMA = "UNEXPECTED_SCHEMA"
MISSING_EVENT_ID = "MISSING_EVENT_ID"
INVALID_EVENT_ID = "INVALID_EVENT_ID"
MISSING_TIMESTAMP = "MISSING_TIMESTAMP"
INVALID_TIMESTAMP = "INVALID_TIMESTAMP"
TIMESTAMP_OUT_OF_RANGE = "TIMESTAMP_OUT_OF_RANGE"
MISSING_SEVERITY = "MISSING_SEVERITY"
INVALID_SEVERITY = "INVALID_SEVERITY"
MISSING_SERVICE_NAME = "MISSING_SERVICE_NAME"
UNKNOWN_SERVICE = "UNKNOWN_SERVICE"
MISSING_MESSAGE = "MISSING_MESSAGE"
INVALID_LATENCY_TYPE = "INVALID_LATENCY_TYPE"
NEGATIVE_LATENCY = "NEGATIVE_LATENCY"
LATENCY_EXCEEDS_MAX = "LATENCY_EXCEEDS_MAX"
DUPLICATE_EVENT_ID_CONFLICT = "DUPLICATE_EVENT_ID_CONFLICT"

_EVENT_ID_RE = re.compile(config.EVENT_ID_PATTERN)
_STRING_FIELDS = ("event_id", "timestamp", "severity", "service_name", "message")
_STD_KEYS = frozenset(config.STANDARD_FIELDS)


def check_json_schema(obj) -> Optional[str]:
    """Return a description if a parsed JSON value does not look like a log event, else None."""
    if not isinstance(obj, dict):
        return f"expected a JSON object, got {type(obj).__name__}"
    if config.FAST_PATHS and obj and obj.keys() <= _STD_KEYS:      # common case: only standard key names
        for name in _STRING_FIELDS:
            if isinstance(obj.get(name), (dict, list)):
                return f"field '{name}' has unsupported type {type(obj[name]).__name__}"
        return None
    keys = {str(k).strip().lower(): k for k in obj}
    known = {k: config.FIELD_ALIASES[k] for k in keys if k in config.FIELD_ALIASES}
    if not known:
        return "no recognised log fields (event_id/timestamp/severity/service_name/message/latency_ms)"
    for lower, orig in keys.items():
        std = config.FIELD_ALIASES.get(lower)
        if std in _STRING_FIELDS and isinstance(obj[orig], (dict, list)):
            return f"field '{orig}' has unsupported type {type(obj[orig]).__name__}"
    return None


def validate_record(rec: dict, known_services: Optional[Set[str]] = None) -> List[str]:
    """Run every rule on a normalised record and return ALL rejection reasons (empty = valid)."""
    if known_services is None:
        known_services = set(config.KNOWN_SERVICES)
    reasons: List[str] = []

    # event id
    if rec.get("event_id") is None:
        reasons.append(MISSING_EVENT_ID)
    elif not _EVENT_ID_RE.match(rec["event_id"]):
        reasons.append(INVALID_EVENT_ID)

    # timestamp
    if rec.get("timestamp_raw") is None:
        reasons.append(MISSING_TIMESTAMP)
    elif rec.get("event_timestamp") is None:
        reasons.append(INVALID_TIMESTAMP)
    elif not (config.MIN_TIMESTAMP_EPOCH <= rec["event_epoch"] < config.MAX_TIMESTAMP_EPOCH):
        reasons.append(TIMESTAMP_OUT_OF_RANGE)

    # severity
    if rec.get("severity_raw") is None:
        reasons.append(MISSING_SEVERITY)
    elif rec.get("severity") is None:
        reasons.append(INVALID_SEVERITY)

    # service (+ referential integrity against the services reference table)
    if rec.get("service_name") is None:
        reasons.append(MISSING_SERVICE_NAME)
    elif rec["service_name"] not in known_services:
        reasons.append(UNKNOWN_SERVICE)

    # message
    if rec.get("message") is None:
        reasons.append(MISSING_MESSAGE)

    # latency (optional, but must be sane when present)
    status = rec.get("latency_status")
    if status == "invalid":
        reasons.append(INVALID_LATENCY_TYPE)
    elif status == "ok":
        if rec["latency_ms"] < 0:
            reasons.append(NEGATIVE_LATENCY)
        elif rec["latency_ms"] > config.MAX_LATENCY_MS:
            reasons.append(LATENCY_EXCEEDS_MAX)
    return reasons


def split_valid_invalid(records: Iterable[dict], known_services: Optional[Set[str]] = None
                        ) -> Tuple[List[dict], List[Tuple[dict, List[str]]]]:
    valid, invalid = [], []
    for rec in records:
        reasons = validate_record(rec, known_services)
        (invalid.append((rec, reasons)) if reasons else valid.append(rec))
    return valid, invalid


def detect_duplicates(valid: Iterable[dict], existing: Dict[str, str]
                      ) -> Tuple[List[dict], List[dict], List[dict]]:
    """Split valid records into (new, duplicates, conflicts).

    * duplicate  - event_id already known (earlier in this batch or in the database)
                   with the SAME payload  -> counted, not stored again.
    * conflict   - event_id already known but with DIFFERENT content -> quarantined.
    First occurrence wins.
    """
    seen = dict(existing)
    new, dups, conflicts = [], [], []
    for rec in valid:
        known_hash = seen.get(rec["event_id"])
        if known_hash is None:
            seen[rec["event_id"]] = rec["payload_hash"]
            new.append(rec)
        elif known_hash == rec["payload_hash"]:
            dups.append(rec)
        else:
            conflicts.append(rec)
    return new, dups, conflicts


def build_quarantine_record(raw, reasons: List[str], stage: str, run_id: str,
                            event_id: Optional[str] = None) -> dict:
    """Create the row stored in ``rejected_logs`` (raw line preserved verbatim)."""
    key = hashlib.sha1(f"{raw.file_sha256}|{raw.line_number}|{raw.raw}".encode("utf-8")).hexdigest()
    return {
        "reject_key": key, "run_id": run_id, "stage": stage,
        "primary_reason": reasons[0], "rejection_reasons": ";".join(reasons),
        "event_id": event_id, "source_format": raw.source_format,
        "source_file": raw.source_file, "source_line": raw.line_number,
        "file_sha256": raw.file_sha256, "raw_record": raw.raw, "rejected_at": utc_now_iso(),
    }
