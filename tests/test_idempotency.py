"""Idempotency is the headline requirement: reruns must never create duplicates or drift."""
import json

from src import config
from src.generator import DATASETS
from tests.helpers import events, jline, open_db, run, write


def sample_files():
    s = DATASETS["sample"]
    return [config.SAMPLE_DIR / s["json"], config.SAMPLE_DIR / s["text"]]


def snapshot(tmp_path):
    st = open_db(tmp_path)
    rows = st.conn.execute("SELECT event_id, payload_hash, run_id FROM processed_logs ORDER BY event_id").fetchall()
    rej = st.conn.execute("SELECT reject_key FROM rejected_logs ORDER BY reject_key").fetchall()
    st.close()
    return [tuple(r) for r in rows], [tuple(r) for r in rej]


def test_first_then_second_run_on_sample_dataset(tmp_path):
    first = run(tmp_path, sample_files())
    second = run(tmp_path, sample_files())
    a, b = first.metrics, second.metrics
    assert (a.input_records, a.inserted_records, a.duplicate_records) == (100, 84, 2)
    assert (b.input_records, b.inserted_records) == (100, 0)
    assert b.duplicate_records == 84 + 2                        # every valid record is now a duplicate
    assert b.rejected_records == a.rejected_records == 14      # same bad records, quarantined once only


def test_rerun_leaves_database_and_quarantine_unchanged(tmp_path):
    run(tmp_path, sample_files())
    before = snapshot(tmp_path)
    run(tmp_path, sample_files())
    run(tmp_path, sample_files())
    assert snapshot(tmp_path) == before                         # same rows, same run_id on every row
    assert len(before[0]) == 84 and len(before[1]) == 14


def test_rerun_produces_identical_analytics(tmp_path):
    r1 = run(tmp_path, sample_files())
    r2 = run(tmp_path, sample_files())
    for key in ("error_analytics", "latency_analytics"):
        assert r1.report[key] == r2.report[key]


def test_file_checksum_tracking(tmp_path):
    run(tmp_path, sample_files()); run(tmp_path, sample_files())
    st = open_db(tmp_path)
    rows = st.conn.execute("SELECT file_name, times_seen, status, record_count FROM processed_files ORDER BY file_name").fetchall()
    st.close()
    assert [(r[0], r[1], r[2]) for r in rows] == [("application_logs.txt", 2, "PROCESSED"), ("structured_logs.jsonl", 2, "PROCESSED")]


def test_skip_processed_short_circuits_known_files(tmp_path):
    run(tmp_path, sample_files())
    res = run(tmp_path, sample_files(), skip_processed=True)
    m = res.metrics
    assert m.files_skipped == 2 and m.input_records == 0 and m.status == "SUCCESS"


def test_renamed_copy_of_same_content_is_still_recognised(tmp_path):
    lines = events("A", 5)
    run(tmp_path, [write(tmp_path, "one.jsonl", lines)])
    res = run(tmp_path, [write(tmp_path, "copy_of_one.jsonl", lines)])
    assert res.metrics.inserted_records == 0 and res.metrics.duplicate_records == 5


def test_backfill_only_inserts_new_events(tmp_path):
    run(tmp_path, [write(tmp_path, "day1.jsonl", events("A", 5))])
    res = run(tmp_path, [write(tmp_path, "day1_plus.jsonl", events("A", 5) + events("B", 3, start_min=30))])
    assert res.metrics.inserted_records == 3 and res.metrics.duplicate_records == 5


def test_result_is_independent_of_file_order(tmp_path):
    f1 = write(tmp_path, "f1.jsonl", events("A", 5) + events("S", 2, start_min=40))
    f2 = write(tmp_path, "f2.jsonl", events("B", 5, start_min=10) + events("S", 2, start_min=40))
    run(tmp_path, [f1, f2], db="x.db"); run(tmp_path, [f2, f1], db="y.db")
    ids = lambda db: [r[0] for r in open_db(tmp_path, db).conn.execute("SELECT event_id FROM processed_logs ORDER BY 1")]
    assert ids("x.db") == ids("y.db") and len(ids("x.db")) == 12


def test_text_events_dedupe_across_files_via_deterministic_ids(tmp_path):
    line = "2026-11-05 10:16:30 ERROR [payment] Database connection failed latency_ms=2500"
    run(tmp_path, [write(tmp_path, "a.log", [line])])
    res = run(tmp_path, [write(tmp_path, "b.log", [line])])
    assert res.metrics.inserted_records == 0 and res.metrics.duplicate_records == 1


def test_same_event_id_with_different_content_is_quarantined_not_overwritten(tmp_path):
    run(tmp_path, [write(tmp_path, "a.jsonl", [jline(event_id="X", message="original")])])
    res = run(tmp_path, [write(tmp_path, "b.jsonl", [jline(event_id="X", message="changed")])])
    assert res.metrics.inserted_records == 0 and res.metrics.rejected_by_reason == {"DUPLICATE_EVENT_ID_CONFLICT": 1}
    st = open_db(tmp_path)
    assert st.conn.execute("SELECT message FROM processed_logs WHERE event_id='X'").fetchone()[0] == "original"
    assert st.count("rejected_logs") == 1


def test_documented_expected_behaviour_numbers(tmp_path):
    """First run: Inserted N / Duplicates 0.  Identical second run: Inserted 0 / Duplicates N."""
    f = write(tmp_path, "n.jsonl", events("N", 1000, ))
    a, b = run(tmp_path, [f]).metrics, run(tmp_path, [f]).metrics
    assert (a.input_records, a.inserted_records, a.duplicate_records) == (1000, 1000, 0)
    assert (b.input_records, b.inserted_records, b.duplicate_records) == (1000, 0, 1000)


def test_skip_processed_run_still_verifies_database_state(tmp_path):
    spec = DATASETS["sample"]
    exp = spec["dir"] / spec["expected"]
    run(tmp_path, sample_files(), expected_path=exp)
    res = run(tmp_path, sample_files(), skip_processed=True, expected_path=exp)
    assert res.reconciliation["expected_results_passed"] is True       # DB state matches; run counts N/A
