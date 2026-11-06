"""Normalisation layer: consistent timestamps (UTC), severities, numbers, names and IDs."""
from __future__ import annotations

import hashlib
import math
import re
from functools import lru_cache
from datetime import datetime, timedelta, timezone
from typing import List, Optional, Tuple

from . import config
from .parser import ParsedRecord

_TS_RE = re.compile(
    r"^(\d{4})-(\d{2})-(\d{2})[T ](\d{2}):(\d{2}):(\d{2})(?:\.\d{1,9})?\s*(Z|z|[+-]\d{2}:?\d{2})?$"
)
_NUM_RE = re.compile(r"^[+-]?(\d+(\.\d*)?|\.\d+)([eE][+-]?\d+)?\s*(ms)?$", re.IGNORECASE)


@lru_cache(maxsize=8192)
def _clean_str(value: str) -> Optional[str]:
    text = " ".join(value.split())
    return None if text.lower() in config.MISSING_TOKENS else text


def clean_text(value) -> Optional[str]:
    """Trim, collapse whitespace and standardise missing-value tokens to None."""
    if value is None:
        return None
    if config.FAST_PATHS and type(value) is str:
        return _clean_str(value)          # severities/services/messages repeat a lot -> cache hits
    text = " ".join(str(value).split())
    return None if text.lower() in config.MISSING_TOKENS else text


def parse_timestamp(value) -> Optional[datetime]:
    """Strict timestamp parser -> aware UTC datetime, or None when the format is invalid.

    Accepted: ISO-8601 ``YYYY-MM-DD[T| ]HH:MM:SS[.fff][Z|+HH:MM]``. No zone means UTC.
    """
    if not isinstance(value, str):
        return None
    v = value
    if (config.FAST_PATHS and len(v) == 20 and v[19] == "Z" and v[10] == "T" and v[4] == "-" and v[7] == "-"
            and v[13] == ":" and v[16] == ":"):
        try:   # canonical 'YYYY-MM-DDTHH:MM:SSZ' - by far the most common shape
            return datetime(int(v[0:4]), int(v[5:7]), int(v[8:10]), int(v[11:13]), int(v[14:16]),
                            int(v[17:19]), tzinfo=timezone.utc)
        except ValueError:
            pass                          # fall through to the strict general parser
    m = _TS_RE.match(value.strip())
    if not m:
        return None
    y, mo, d, h, mi, s = (int(x) for x in m.groups()[:6])
    try:
        dt = datetime(y, mo, d, h, mi, s, tzinfo=timezone.utc)
    except ValueError:
        return None
    tz = m.group(7)
    if tz and tz.upper() != "Z":
        sign = 1 if tz[0] == "+" else -1
        digits = tz[1:].replace(":", "")
        oh, om = int(digits[:2]), int(digits[2:])
        if oh > 23 or om > 59:
            return None
        dt -= sign * timedelta(hours=oh, minutes=om)   # local = utc + offset
    return dt


def normalize_timestamp(value) -> Tuple[Optional[str], Optional[float]]:
    """Return (``YYYY-MM-DDTHH:MM:SSZ``, epoch seconds) or (None, None)."""
    dt = parse_timestamp(value)
    if dt is None:
        return None, None
    return dt.isoformat().replace("+00:00", "Z"), dt.timestamp()


def normalize_severity(value) -> Optional[str]:
    text = clean_text(value)
    return None if text is None else config.SEVERITY_ALIASES.get(text.lower())


def normalize_service(value) -> Optional[str]:
    text = clean_text(value)
    return None if text is None else text.lower()


def to_latency(value) -> Tuple[Optional[float], str]:
    """Convert to float milliseconds. Status: ``ok`` | ``missing`` | ``invalid``."""
    if value is None:
        return None, "missing"
    if isinstance(value, bool) or isinstance(value, (dict, list)):
        return None, "invalid"
    if isinstance(value, (int, float)):
        return (float(value), "ok") if math.isfinite(value) else (None, "invalid")
    text = clean_text(value)
    if text is None:
        return None, "missing"
    if not _NUM_RE.match(text):
        return None, "invalid"
    number = float(re.sub(r"(?i)\s*ms$", "", text))
    return (number, "ok") if math.isfinite(number) else (None, "invalid")


_STD_FIELDS = frozenset(config.STANDARD_FIELDS)


def standardize_columns(fields: dict) -> Tuple[dict, List[str]]:
    """Map source keys (``level``, ``service``, ``msg`` ...) onto the standard column names."""
    if config.FAST_PATHS and fields.keys() <= _STD_FIELDS:
        return dict(fields), []           # already standard names: nothing to map
    std, extras = {}, []
    for key, value in fields.items():                       # exact standard names win
        k = str(key).strip().lower()
        if k in config.STANDARD_FIELDS:
            std[k] = value
    for key, value in fields.items():
        k = str(key).strip().lower()
        target = config.FIELD_ALIASES.get(k)
        if target is None:
            extras.append(str(key))
        elif k not in config.STANDARD_FIELDS and target not in std:
            std[target] = value
    return std, extras


def compute_payload_hash(ts: Optional[str], sev: Optional[str], svc: Optional[str],
                         msg: Optional[str], lat: Optional[float]) -> str:
    lat_s = "" if lat is None else format(lat, ".6f")
    joined = "|".join(["" if x is None else x for x in (ts, sev, svc, msg)] + [lat_s])
    return hashlib.sha256(joined.encode("utf-8")).hexdigest()[:32]


def normalize_record(parsed: ParsedRecord) -> dict:
    """ParsedRecord -> flat, consistently typed record (may still be invalid; validation decides)."""
    fields, extras = standardize_columns(parsed.fields)
    ts_raw = clean_text(fields.get("timestamp"))
    ts_norm, epoch = normalize_timestamp(ts_raw)
    sev_raw = clean_text(fields.get("severity"))
    svc = normalize_service(fields.get("service_name"))
    msg = clean_text(fields.get("message"))
    lat, lat_status = to_latency(fields.get("latency_ms"))
    sev = normalize_severity(sev_raw)
    payload_hash = compute_payload_hash(ts_norm, sev, svc, msg, lat)

    event_id = clean_text(fields.get("event_id"))
    generated = 0
    if event_id is None and parsed.raw.source_format == "text":
        # The text format carries no ID: derive a deterministic one from the content.
        event_id, generated = "GEN-" + payload_hash[:16], 1

    raw = parsed.raw
    return {
        "event_id": event_id, "timestamp_raw": ts_raw, "event_timestamp": ts_norm, "event_epoch": epoch,
        "severity_raw": sev_raw, "severity": sev, "service_name": svc, "message": msg,
        "latency_raw": fields.get("latency_ms"), "latency_ms": lat, "latency_status": lat_status,
        "source_format": raw.source_format, "source_file": raw.source_file, "source_line": raw.line_number,
        "file_sha256": raw.file_sha256, "event_id_generated": generated, "payload_hash": payload_hash,
        "extra_fields": extras, "_raw": raw,
    }


def derive_fields(rec: dict) -> dict:
    """Add derived columns for valid records."""
    ts = rec["event_timestamp"]
    rec["event_date"] = ts[:10]
    rec["event_hour"] = ts[:13] + ":00:00Z"
    rec["is_error"] = int(rec["severity"] in config.ERROR_SEVERITIES)
    rec.setdefault("is_late", 0)
    return rec


def flag_late_and_out_of_order(records: List[dict], db_watermark_epoch: Optional[float],
                               threshold_minutes: float = config.LATE_THRESHOLD_MINUTES) -> Tuple[int, int]:
    """Annotate ``is_late`` in place; return (late_count, out_of_order_count).

    * out-of-order: timestamp older than the newest event seen earlier in the SAME file.
    * late:        older than max(file running max, database watermark) by more than the threshold.
    Late events are kept (they are valid) - analytics bucket them by event time.
    """
    threshold = threshold_minutes * 60
    running = None
    late = ooo = 0
    for rec in records:
        epoch = rec["event_epoch"]
        if running is not None and epoch < running:
            ooo += 1
        marks = [m for m in (running, db_watermark_epoch) if m is not None]
        reference = max(marks) if marks else None
        rec["is_late"] = int(reference is not None and (reference - epoch) > threshold)
        late += rec["is_late"]
        running = epoch if running is None else max(running, epoch)
    return late, ooo
