import pandas as pd
import pytest

from src.analytics import error_analytics, latency_analytics


def df(rows):
    d = pd.DataFrame(rows, columns=["event_timestamp", "severity", "service_name", "latency_ms"])
    d["event_date"] = d["event_timestamp"].str[:10]
    d["event_hour"] = d["event_timestamp"].str[:13] + ":00:00Z"
    d["is_error"] = d["severity"].isin(["ERROR", "CRITICAL"]).astype(int)
    return d


ROWS = [("2026-11-05T10:00:00Z", "INFO", "auth", 100), ("2026-11-05T10:10:00Z", "INFO", "auth", 200),
        ("2026-11-05T11:00:00Z", "ERROR", "payment", 1000), ("2026-11-06T09:00:00Z", "CRITICAL", "payment", 3000),
        ("2026-11-06T09:30:00Z", "WARNING", "auth", None)]


def test_error_rate_formula_and_counts():
    ea = error_analytics(df(ROWS))
    assert ea["total_logs"] == 5 and ea["severity_counts"] == {"INFO": 2, "WARNING": 1, "ERROR": 1, "CRITICAL": 1}
    assert ea["error_count"] == 2 and ea["error_rate_pct"] == 40.0       # (1+1)/5*100


def test_error_rate_by_service_and_trends():
    ea = error_analytics(df(ROWS))
    assert ea["error_rate_by_service"]["payment"] == {"total": 2, "errors": 2, "error_rate_pct": 100.0}
    assert ea["error_rate_by_service"]["auth"]["error_rate_pct"] == 0.0
    daily = {t["period"]: t for t in ea["error_trend"]["daily"]}
    assert daily["2026-11-05"]["errors"] == 1 and daily["2026-11-06"]["total"] == 2
    assert len(ea["error_trend"]["hourly"]) == 3        # 10:00, 11:00 and 09:00 next day


def test_latency_stats_ignore_nulls():
    la = latency_analytics(df(ROWS))
    assert la["count"] == 4 and la["avg_ms"] == 1075.0 and la["min_ms"] == 100.0 and la["max_ms"] == 3000.0
    assert la["p50_ms"] == 600.0                                   # median of 100,200,1000,3000
    assert la["by_service"]["auth"]["count"] == 2


def test_p95_uses_linear_interpolation():
    la = latency_analytics(df([("2026-11-05T10:00:00Z", "INFO", "auth", i) for i in range(1, 101)]))
    assert la["p95_ms"] == pytest.approx(95.05) and la["p50_ms"] == pytest.approx(50.5)


def test_empty_dataframe_does_not_crash():
    empty = df([])
    ea, la = error_analytics(empty), latency_analytics(empty)
    assert ea["total_logs"] == 0 and ea["error_rate_pct"] == 0.0 and la["avg_ms"] is None
