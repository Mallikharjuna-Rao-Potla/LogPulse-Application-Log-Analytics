import pytest

from src import validation as v
from src.validation import build_quarantine_record, detect_duplicates, split_valid_invalid, validate_record
from tests.helpers import jline, norm, raw


def reasons(**kw):
    drop = kw.pop("drop", ())
    return validate_record(norm(jline(drop=drop, **kw)))


def test_valid_record_has_no_reasons():
    assert reasons() == []


@pytest.mark.parametrize("kwargs,expected", [
    (dict(drop=["event_id"]), v.MISSING_EVENT_ID),
    (dict(event_id=None), v.MISSING_EVENT_ID),
    (dict(event_id="bad id!"), v.INVALID_EVENT_ID),
    (dict(drop=["timestamp"]), v.MISSING_TIMESTAMP),
    (dict(timestamp=""), v.MISSING_TIMESTAMP),
    (dict(timestamp="05/11/2026 10:17:30"), v.INVALID_TIMESTAMP),
    (dict(timestamp="1999-12-31T23:59:59Z"), v.TIMESTAMP_OUT_OF_RANGE),
    (dict(severity=None), v.MISSING_SEVERITY),
    (dict(severity="DEBUG"), v.INVALID_SEVERITY),
    (dict(service_name=None), v.MISSING_SERVICE_NAME),
    (dict(service_name="  "), v.MISSING_SERVICE_NAME),
    (dict(service_name="legacy-billing"), v.UNKNOWN_SERVICE),
    (dict(message=None), v.MISSING_MESSAGE),
    (dict(latency_ms="abc"), v.INVALID_LATENCY_TYPE),
    (dict(latency_ms=-100), v.NEGATIVE_LATENCY),
    (dict(latency_ms=99_999_999), v.LATENCY_EXCEEDS_MAX),
])
def test_single_defect_gives_single_reason(kwargs, expected):
    assert reasons(**kwargs) == [expected]


def test_null_latency_is_allowed_it_is_optional():
    assert reasons(latency_ms=None) == [] and reasons(drop=["latency_ms"]) == []


def test_multiple_defects_report_all_reasons():
    got = reasons(severity="DEBUG", latency_ms=-1, service_name=None)
    assert set(got) == {v.INVALID_SEVERITY, v.NEGATIVE_LATENCY, v.MISSING_SERVICE_NAME}


def test_referential_integrity_uses_supplied_reference_set():
    rec = norm(jline(service_name="mailer"))
    assert validate_record(rec, {"auth"}) == [v.UNKNOWN_SERVICE]
    assert validate_record(rec, {"auth", "mailer"}) == []


def test_split_valid_invalid_keeps_everything():
    recs = [norm(jline(event_id="a")), norm(jline(event_id="b", severity="x")), norm(jline(event_id="c"))]
    good, bad = split_valid_invalid(recs)
    assert len(good) == 2 and len(bad) == 1 and bad[0][1] == [v.INVALID_SEVERITY]


def test_duplicate_detection_in_batch_and_against_database():
    a, a2, b = norm(jline(event_id="A")), norm(jline(event_id="A")), norm(jline(event_id="B"))
    new, dups, conflicts = detect_duplicates([a, a2, b], existing={})
    assert [r["event_id"] for r in new] == ["A", "B"] and len(dups) == 1 and conflicts == []
    new, dups, conflicts = detect_duplicates([b], existing={"B": b["payload_hash"]})
    assert new == [] and len(dups) == 1


def test_same_id_different_content_is_a_conflict():
    a, other = norm(jline(event_id="A")), norm(jline(event_id="A", message="different"))
    new, dups, conflicts = detect_duplicates([a, other], existing={})
    assert len(new) == 1 and dups == [] and len(conflicts) == 1


def test_quarantine_record_preserves_raw_line_and_key_is_stable():
    r = raw('{"broken"', n=7)
    q1 = build_quarantine_record(r, [v.PARSE_ERROR_MALFORMED_JSON], "PARSING", "run1")
    q2 = build_quarantine_record(r, [v.PARSE_ERROR_MALFORMED_JSON], "PARSING", "run2")
    assert q1["raw_record"] == '{"broken"' and q1["source_line"] == 7
    assert q1["reject_key"] == q2["reject_key"]          # run id does not change identity -> idempotent
    assert q1["primary_reason"] == "PARSE_ERROR_MALFORMED_JSON" and q1["stage"] == "PARSING"
