import pytest

from src.transformation import (clean_text, compute_payload_hash, derive_fields, flag_late_and_out_of_order,
                                normalize_record, normalize_service, normalize_severity, normalize_timestamp,
                                standardize_columns, to_latency)
from tests.helpers import jline, norm, raw


@pytest.mark.parametrize("src,expected", [
    ("2026-11-05T10:15:30Z", "2026-11-05T10:15:30Z"),
    ("2026-11-05 10:15:30", "2026-11-05T10:15:30Z"),          # no zone -> UTC
    ("2026-11-05T10:15:30+05:30", "2026-11-05T04:45:30Z"),    # converted to UTC
    ("2026-11-05T10:15:30-08:00", "2026-11-05T18:15:30Z"),
    ("2026-11-05T10:15:30.987Z", "2026-11-05T10:15:30Z"),
    ("  2026-11-05T23:59:59z ", "2026-11-05T23:59:59Z"),
])
def test_timestamp_normalization(src, expected):
    assert normalize_timestamp(src)[0] == expected


@pytest.mark.parametrize("bad", ["05/11/2026 10:17:30", "2026-13-45T10:00:00Z", "yesterday", "2026-11-05 25:61:00",
                                 "20261105", "", None, 12345, "2026-11-05T10:15:30+25:00"])
def test_invalid_timestamps_become_none(bad):
    assert normalize_timestamp(bad) == (None, None)


@pytest.mark.parametrize("src,expected", [("err", "ERROR"), ("warn", "WARNING"), ("info", "INFO"),
                                          ("critical", "CRITICAL"), (" Warning ", "WARNING"), ("ERROR", "ERROR"),
                                          ("DEBUG", None), ("", None), (None, None)])
def test_severity_normalization(src, expected):
    assert normalize_severity(src) == expected


@pytest.mark.parametrize("src,value,status", [
    (120, 120.0, "ok"), ("120", 120.0, "ok"), (" 45.5 ", 45.5, "ok"), ("120ms", 120.0, "ok"), ("-100", -100.0, "ok"),
    (None, None, "missing"), ("", None, "missing"), ("N/A", None, "missing"),
    ("abc", None, "invalid"), (True, None, "invalid"), (float("nan"), None, "invalid"), ([1], None, "invalid"),
])
def test_numeric_conversion(src, value, status):
    assert to_latency(src) == (value, status)


def test_whitespace_and_missing_value_standardisation():
    assert clean_text("  User   login \t ok ") == "User login ok"
    for token in ["", "   ", "null", "None", "NaN", "n/a", "-"]:
        assert clean_text(token) is None
    assert normalize_service("  AUTH ") == "auth"


def test_column_name_standardisation():
    std, extras = standardize_columns({"Level": "INFO", "service": "auth", "msg": "hi", "ts": "t", "id": "1", "host": "h"})
    assert std == {"severity": "INFO", "service_name": "auth", "message": "hi", "timestamp": "t", "event_id": "1"}
    assert extras == ["host"]


def test_standard_name_wins_over_alias():
    std, _ = standardize_columns({"severity": "ERROR", "level": "INFO"})
    assert std["severity"] == "ERROR"


def test_normalize_record_json_end_to_end():
    r = norm(jline(severity="err", service_name=" Payment ", message=" a   b ", latency_ms="250",
                   timestamp="2026-11-05T10:00:00+01:00"))
    assert (r["severity"], r["service_name"], r["message"], r["latency_ms"]) == ("ERROR", "payment", "a b", 250.0)
    assert r["event_timestamp"] == "2026-11-05T09:00:00Z" and r["event_id"] == "E1" and not r["event_id_generated"]


def test_missing_values_stay_none_for_validation():
    r = norm(jline(drop=["service_name"], message="  ", latency_ms=None))
    assert r["service_name"] is None and r["message"] is None and r["latency_status"] == "missing"


def test_deterministic_generated_ids_for_text():
    line = "2026-11-05 10:16:30 ERROR [payment] Database connection failed latency_ms=2500"
    a, b = norm(line, "text"), norm(line, "text")
    assert a["event_id"] == b["event_id"] and a["event_id"].startswith("GEN-") and a["event_id_generated"] == 1
    assert norm(line.replace("2500", "2501"), "text")["event_id"] != a["event_id"]
    # same event written with a severity alias gets the same ID (hash uses normalised values)
    assert norm(line.replace("ERROR", "err"), "text")["event_id"] == a["event_id"]


def test_json_never_gets_generated_id():
    assert norm(jline(event_id=None))["event_id"] is None


def test_payload_hash_changes_with_content():
    base = compute_payload_hash("t", "INFO", "auth", "m", 1.0)
    assert base == compute_payload_hash("t", "INFO", "auth", "m", 1.0)
    assert base != compute_payload_hash("t", "INFO", "auth", "m", 2.0)


def test_derived_fields():
    r = derive_fields(norm(jline(severity="CRITICAL")))
    assert (r["event_date"], r["event_hour"], r["is_error"]) == ("2026-11-05", "2026-11-05T10:00:00Z", 1)


def _recs(*times):
    return [{"event_epoch": float(t)} for t in times]


def test_out_of_order_and_late_flags():
    recs = _recs(1000, 2000, 1900, 9000, 3000)          # 1900 & 3000 arrive after newer events
    late, ooo = flag_late_and_out_of_order(recs, None, threshold_minutes=30)
    assert ooo == 2 and late == 1                       # 3000 is >1800s older than 9000
    assert [r["is_late"] for r in recs] == [0, 0, 0, 0, 1]


def test_late_relative_to_database_watermark():
    recs = _recs(100)
    late, ooo = flag_late_and_out_of_order(recs, db_watermark_epoch=100 + 3600, threshold_minutes=30)
    assert late == 1 and ooo == 0                       # not out-of-order within file, but late vs warehouse
