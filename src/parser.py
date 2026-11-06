"""Parsing layer: turn a raw line into a ParsedRecord (never raises, never loses the raw line)."""
from __future__ import annotations

import json
import re
from dataclasses import dataclass
from typing import List, Optional

from .ingestion import RawRecord
from .validation import (PARSE_ERROR_MALFORMED_JSON, PARSE_ERROR_UNEXPECTED_FORMAT,
                         UNEXPECTED_SCHEMA, check_json_schema)

# 2026-11-05 10:16:30 ERROR [payment] Database connection failed latency_ms=2500
TEXT_RE = re.compile(
    r"^\s*(?P<date>\S+)\s+(?P<time>\S+)\s+(?P<severity>\S+)\s+\[(?P<service>[^\]]*)\]\s*(?P<rest>.*)$"
)
# trailing key=value tokens we understand (optional in the format)
TAIL_KV_RE = re.compile(r"(?:^|\s)(?P<key>latency_ms|event_id)=(?P<value>\S*)\s*$")


@dataclass
class ParsedRecord:
    raw: RawRecord
    fields: Optional[dict] = None   # source keys as found (JSON) or standard keys (text)
    error_code: Optional[str] = None
    error_detail: Optional[str] = None

    @property
    def ok(self) -> bool:
        return self.error_code is None


def parse_json_line(raw: RawRecord) -> ParsedRecord:
    try:
        obj = json.loads(raw.raw)
    except (ValueError, RecursionError) as exc:  # JSONDecodeError subclasses ValueError
        return ParsedRecord(raw, error_code=PARSE_ERROR_MALFORMED_JSON, error_detail=str(exc)[:200])
    problem = check_json_schema(obj)
    if problem:
        return ParsedRecord(raw, error_code=UNEXPECTED_SCHEMA, error_detail=problem)
    return ParsedRecord(raw, fields=obj)


def parse_text_line(raw: RawRecord) -> ParsedRecord:
    m = TEXT_RE.match(raw.raw)
    if not m:
        return ParsedRecord(raw, error_code=PARSE_ERROR_UNEXPECTED_FORMAT,
                            error_detail="line does not match '<date> <time> <SEVERITY> [service] message [latency_ms=N]'")
    rest = m.group("rest")
    extracted = {}
    for _ in range(2):  # latency_ms and/or event_id may trail the message
        tail = TAIL_KV_RE.search(rest)
        if not tail:
            break
        extracted.setdefault(tail.group("key"), tail.group("value"))
        rest = rest[:tail.start()]
    fields = {
        "event_id": extracted.get("event_id"),   # absent in the standard text format
        "timestamp": f"{m.group('date')} {m.group('time')}",
        "severity": m.group("severity"),
        "service_name": m.group("service"),
        "message": rest,
        "latency_ms": extracted.get("latency_ms"),
    }
    return ParsedRecord(raw, fields=fields)


def parse_record(raw: RawRecord) -> ParsedRecord:
    """Dispatch on the detected format; any unexpected exception becomes a parse failure."""
    try:
        return parse_json_line(raw) if raw.source_format == "json" else parse_text_line(raw)
    except Exception as exc:  # defensive: a parser bug must not kill the whole file
        return ParsedRecord(raw, error_code=PARSE_ERROR_UNEXPECTED_FORMAT,
                            error_detail=f"{type(exc).__name__}: {exc}"[:200])


def parse_records(raws: List[RawRecord]) -> List[ParsedRecord]:
    return [parse_record(r) for r in raws]
