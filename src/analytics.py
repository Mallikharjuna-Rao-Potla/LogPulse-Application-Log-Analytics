"""Analytics layer: error, latency and operational metrics (pandas, computed from the database)."""
from __future__ import annotations

from typing import Optional

import numpy as np
import pandas as pd

from . import config
from .logger import utc_now_iso

LOG_QUERY = """SELECT event_id, event_timestamp, event_date, event_hour, severity, service_name,
                      latency_ms, is_error, is_late FROM processed_logs"""


def load_processed_df(conn) -> pd.DataFrame:
    df = pd.read_sql_query(LOG_QUERY, conn)
    df["latency_ms"] = pd.to_numeric(df["latency_ms"], errors="coerce")
    return df


def _num(x, digits: int = 4):
    return None if x is None or pd.isna(x) else round(float(x), digits)


def p95(s: pd.Series):
    return s.quantile(0.95)          # linear interpolation (same as numpy default)


def _latency_stats(s: pd.Series) -> dict:
    s = s.dropna()
    if s.empty:
        return {"count": 0, "avg_ms": None, "min_ms": None, "max_ms": None, "p50_ms": None, "p95_ms": None}
    return {"count": int(s.size), "avg_ms": _num(s.mean()), "min_ms": _num(s.min()), "max_ms": _num(s.max()),
            "p50_ms": _num(s.median()), "p95_ms": _num(p95(s))}


def error_analytics(df: pd.DataFrame) -> dict:
    """Error Rate = (ERROR + CRITICAL) / Total Valid Logs x 100."""
    total = int(len(df))
    counts = {sev: int((df["severity"] == sev).sum()) for sev in config.VALID_SEVERITIES}
    errors = counts["ERROR"] + counts["CRITICAL"]
    rate = lambda e, t: round(e / t * 100, 4) if t else 0.0

    by_service = {}
    for svc, g in df.groupby("service_name"):
        e = int(g["is_error"].sum())
        by_service[svc] = {"total": int(len(g)), "errors": e, "error_rate_pct": rate(e, len(g))}

    def trend(col):
        out = []
        for key, g in df.groupby(col):
            e = int(g["is_error"].sum())
            out.append({"period": key, "total": int(len(g)), "errors": e, "error_rate_pct": rate(e, len(g))})
        return out

    return {"total_logs": total, "severity_counts": counts, "error_count": errors,
            "error_rate_pct": rate(errors, total),
            "error_rate_by_service": dict(sorted(by_service.items())),
            "error_trend": {"daily": trend("event_date"), "hourly": trend("event_hour")}}


def latency_analytics(df: pd.DataFrame) -> dict:
    by_service = {svc: _latency_stats(g["latency_ms"]) for svc, g in sorted(df.groupby("service_name"))}

    def trend(col):
        out = []
        for key, g in df.groupby(col):
            s = _latency_stats(g["latency_ms"])
            out.append({"period": key, "count": s["count"], "avg_ms": s["avg_ms"], "p95_ms": s["p95_ms"]})
        return out

    result = _latency_stats(df["latency_ms"])
    result.update({"by_service": by_service, "trend": {"daily": trend("event_date"), "hourly": trend("event_hour")}})
    return result


def operational_analytics(metrics) -> dict:
    return {
        "run_id": metrics.run_id, "status": metrics.status, "failure_reason": metrics.failure_reason,
        "started_at": metrics.started_at, "ended_at": metrics.ended_at,
        "files_total": metrics.files_total, "files_processed": metrics.files_processed,
        "files_failed": metrics.files_failed, "files_skipped": metrics.files_skipped,
        "blank_lines_skipped": metrics.blank_lines_skipped,
        "total_input_records": metrics.input_records, "parsed_records": metrics.parsed_records,
        "successfully_processed_records": metrics.inserted_records,
        "duplicate_records": metrics.duplicate_records, "rejected_records": metrics.rejected_records,
        "parse_failures": metrics.parse_failures, "validation_failures": metrics.validation_failures,
        "duplicate_id_conflicts": metrics.conflict_rejections,
        "late_records": metrics.late_records, "out_of_order_records": metrics.out_of_order_records,
        "rejected_by_reason": dict(sorted(metrics.rejected_by_reason.items())),
        "processing_seconds": round(metrics.processing_seconds, 4),
        "duration_seconds": round(metrics.duration_seconds, 4),
        "throughput_records_per_sec": metrics.throughput_rps,
    }


def build_report(df: pd.DataFrame, metrics, db_totals: dict, db_path: str) -> dict:
    return {
        "generated_at": utc_now_iso(),
        "database": str(db_path),
        "scope_note": "error/latency analytics cover ALL valid records currently in the database; "
                      "'operational' covers this run only.",
        "operational": operational_analytics(metrics),
        "database_totals": db_totals,
        "late_records_in_database": int(df["is_late"].sum()) if len(df) else 0,
        "error_analytics": error_analytics(df),
        "latency_analytics": latency_analytics(df),
    }
