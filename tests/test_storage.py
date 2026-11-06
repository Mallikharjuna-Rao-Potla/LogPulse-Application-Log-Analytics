import sqlite3

import pytest

from src import storage as storage_mod
from src.storage import Storage, is_transient
from src.validation import build_quarantine_record
from tests.helpers import raw, valid_rec


def make():
    st = Storage(":memory:")
    st.start_run("r1", "2026-11-05T10:00:00Z")
    return st


def test_schema_created_and_services_seeded():
    st = make()
    tables = {r[0] for r in st.conn.execute("SELECT name FROM sqlite_master WHERE type='table'")}
    assert {"processed_logs", "rejected_logs", "pipeline_runs", "processed_files", "services"} <= tables
    assert "payment" in st.get_known_services()


def test_successful_insertion():
    st = make()
    assert st.insert_processed_logs([valid_rec(event_id="a"), valid_rec(event_id="b")], "r1") == 2
    assert st.count("processed_logs") == 2
    row = st.conn.execute("SELECT * FROM processed_logs WHERE event_id='a'").fetchone()
    assert row["severity"] == "INFO" and row["is_error"] == 0 and row["run_id"] == "r1"


@pytest.mark.parametrize("mode", ["batch", "row"])
def test_duplicate_prevention_by_primary_key(mode):
    st = make()
    recs = [valid_rec(event_id="a"), valid_rec(event_id="b")]
    assert st.insert_processed_logs(recs, "r1", mode) == 2
    assert st.insert_processed_logs(recs, "r1", mode) == 0
    assert st.count("processed_logs") == 2


def test_batch_insert_spans_multiple_chunks(monkeypatch):
    monkeypatch.setattr(storage_mod.config, "DB_BATCH_SIZE", 3)
    st = make()
    assert st.insert_processed_logs([valid_rec(event_id=f"e{i}") for i in range(10)], "r1") == 10


def test_lookup_modes_agree():
    st = make()
    recs = [valid_rec(event_id=f"e{i}") for i in range(5)]
    st.insert_processed_logs(recs, "r1")
    ids = [f"e{i}" for i in range(8)]
    assert st.fetch_existing_hashes(ids, "batch") == st.fetch_existing_hashes(ids, "row")
    assert len(st.fetch_existing_hashes(ids)) == 5 and st.fetch_existing_hashes([]) == {}


def _insert_sql(**over):
    base = dict(event_id="x", event_timestamp="t", event_date="d", event_hour="h", severity="INFO",
                service_name="auth", message="m", latency_ms=1.0, is_error=0, is_late=0, source_format="json",
                event_id_generated=0, payload_hash="p", source_file="f", source_line=1, file_sha256="s",
                run_id="r1", ingested_at="t")
    base.update(over)
    cols = ",".join(base)
    return f"INSERT INTO processed_logs ({cols}) VALUES ({','.join('?' * len(base))})", list(base.values())


@pytest.mark.parametrize("override", [
    dict(severity="DEBUG"),            # CHECK: allowed severities
    dict(latency_ms=-5.0),             # CHECK: non-negative latency
    dict(service_name="ghost"),        # FOREIGN KEY -> services
    dict(run_id="no-such-run"),        # FOREIGN KEY -> pipeline_runs
    dict(message=None),                # NOT NULL
    dict(is_error=7),                  # CHECK: boolean flag
])
def test_database_constraints_reject_bad_rows(override):
    st = make()
    sql, args = _insert_sql(**override)
    with pytest.raises(sqlite3.IntegrityError):
        st.conn.execute(sql, args)


def test_transaction_rollback_on_failure():
    st = make()
    with pytest.raises(RuntimeError):
        with st.transaction():
            st.insert_processed_logs([valid_rec(event_id="a")], "r1")
            raise RuntimeError("boom")
    assert st.count("processed_logs") == 0
    with st.transaction():                                   # connection still usable afterwards
        st.insert_processed_logs([valid_rec(event_id="a")], "r1")
    assert st.count("processed_logs") == 1


def test_transaction_rollback_on_constraint_error_is_all_or_nothing():
    st = make()
    good = valid_rec(event_id="good")
    bad = valid_rec(event_id="bad")
    bad["service_name"] = "ghost"
    with pytest.raises(sqlite3.IntegrityError):
        with st.transaction():
            st.insert_processed_logs([good, bad], "r1")
    assert st.count("processed_logs") == 0


def test_quarantine_insert_is_idempotent():
    st = make()
    q = build_quarantine_record(raw("garbage", "text", 3), ["PARSE_ERROR_UNEXPECTED_FORMAT"], "PARSING", "r1")
    assert st.insert_rejected_logs([q]) == (1, 0)
    assert st.insert_rejected_logs([q]) == (0, 1)
    assert st.count("rejected_logs") == 1
    assert st.insert_rejected_logs([]) == (0, 0)


def test_processed_file_tracking_counts_sightings():
    st = make()
    kw = dict(sha256="abc", file_name="f", file_path="/f", size_bytes=1, record_count=2, status="PROCESSED", run_id="r1")
    st.upsert_processed_file(**kw)
    st.upsert_processed_file(**kw)
    row = st.get_processed_file("abc")
    assert row["times_seen"] == 2 and row["status"] == "PROCESSED" and st.count("processed_files") == 1


def test_watermark():
    st = make()
    assert st.max_event_epoch() is None
    st.insert_processed_logs([valid_rec(event_id="a", timestamp="2026-11-05T10:00:00Z"),
                              valid_rec(event_id="b", timestamp="2026-11-05T11:00:00Z")], "r1")
    assert st.max_event_epoch() == 1793876400.0


def test_transient_error_classification():
    assert is_transient(sqlite3.OperationalError("database is locked"))
    assert not is_transient(sqlite3.IntegrityError("UNIQUE constraint failed"))
    assert not is_transient(sqlite3.OperationalError("no such table"))


def test_count_rejects_unknown_table():
    with pytest.raises(ValueError):
        make().count("users; DROP TABLE x")
