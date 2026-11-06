from src.parser import parse_record
from src.validation import PARSE_ERROR_MALFORMED_JSON, PARSE_ERROR_UNEXPECTED_FORMAT, UNEXPECTED_SCHEMA
from tests.helpers import jline, raw


def test_valid_json():
    p = parse_record(raw(jline()))
    assert p.ok and p.fields["event_id"] == "E1" and p.fields["latency_ms"] == 120


def test_valid_text_all_fields():
    p = parse_record(raw("2026-11-05 10:16:30 ERROR [payment] Database connection failed latency_ms=2500", "text"))
    assert p.ok
    assert p.fields == {"event_id": None, "timestamp": "2026-11-05 10:16:30", "severity": "ERROR",
                        "service_name": "payment", "message": "Database connection failed", "latency_ms": "2500"}


def test_text_without_latency_and_with_explicit_event_id():
    p = parse_record(raw("2026-11-05 10:19:30 INFO [auth] User logout successful event_id=EVT9 latency_ms=80", "text"))
    assert p.fields["event_id"] == "EVT9" and p.fields["latency_ms"] == "80"
    assert p.fields["message"] == "User logout successful"
    q = parse_record(raw("2026-11-05 10:19:30 INFO [auth] No latency here", "text"))
    assert q.fields["latency_ms"] is None and q.fields["message"] == "No latency here"


def test_text_extra_whitespace_and_empty_service():
    p = parse_record(raw("2026-11-05   10:19:30   WARN   [ ]   Something odd   ", "text"))
    assert p.ok and p.fields["service_name"] == " "


def test_malformed_json():
    p = parse_record(raw('{"event_id":"E1","timestamp":"2026-11'))
    assert not p.ok and p.error_code == PARSE_ERROR_MALFORMED_JSON and p.fields is None
    assert p.raw.raw.startswith('{"event_id"')           # raw preserved


def test_unexpected_text_format():
    for line in ["### corrupted ###", "ERROR payment Database failed", "{{{{"]:
        p = parse_record(raw(line, "text"))
        assert p.error_code == PARSE_ERROR_UNEXPECTED_FORMAT


def test_json_that_is_not_an_event_is_unexpected_schema():
    for line in ["[1,2,3]", '"text"', "42", '{"foo":"bar"}', '{"message":{"nested":1}}']:
        assert parse_record(raw(line)).error_code == UNEXPECTED_SCHEMA


def test_json_extra_fields_tolerated():
    assert parse_record(raw(jline(extra="x", host="srv1"))).ok


def test_json_text_line_in_text_file_fails_cleanly():
    assert parse_record(raw(jline(), "text")).error_code == PARSE_ERROR_UNEXPECTED_FORMAT
