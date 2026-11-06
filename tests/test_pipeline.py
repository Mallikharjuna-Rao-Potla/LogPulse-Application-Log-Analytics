import json
import sqlite3

from src import config
from src.generator import DATASETS
from src.storage import Storage
from tests.helpers import events, jline, open_db, run, write


def test_happy_path(tmp_path):
    f = write(tmp_path, "ok.jsonl", events("A", 10))
    res = run(tmp_path, [f])
    m = res.metrics
    assert (m.status, m.input_records, m.inserted_records, m.duplicate_records, m.rejected_records) == ("SUCCESS", 10, 10, 0, 0)
    assert res.exit_code == 0 and res.reconciliation["internal_checks_passed"]
    for name in ("processed_logs.csv", "rejected_logs.csv", "analytics_report.json", "reconciliation_report.json"):
        assert (tmp_path / "out" / name).exists()
    run_row = open_db(tmp_path).conn.execute("SELECT status, input_records, inserted_records FROM pipeline_runs").fetchone()
    assert tuple(run_row) == ("SUCCESS", 10, 10)


def test_mixed_json_and_text_inputs(tmp_path):
    j = write(tmp_path, "s.jsonl", [jline(event_id="J1")])
    t = write(tmp_path, "a.txt", ["2026-11-05 10:01:00 WARNING [inventory] Low stock detected latency_ms=450"])
    res = run(tmp_path, [j, t])
    assert res.metrics.inserted_records == 2
    st = open_db(tmp_path)
    assert {r[0] for r in st.conn.execute("SELECT source_format FROM processed_logs")} == {"json", "text"}
    row = st.conn.execute("SELECT event_id, event_id_generated, latency_ms FROM processed_logs WHERE source_format='text'").fetchone()
    assert row[0].startswith("GEN-") and row[1] == 1 and row[2] == 450.0


def test_empty_file_is_success_with_zero_records(tmp_path):
    res = run(tmp_path, [write(tmp_path, "empty.jsonl", [])])
    assert res.status == "SUCCESS" and res.metrics.input_records == 0 and res.metrics.files_processed == 1
    assert res.reconciliation["internal_checks_passed"]


def test_no_input_files_fails(tmp_path):
    res = run(tmp_path, [])
    assert res.status == "FAILED" and "No input files" in res.metrics.failure_reason and res.exit_code == 2


def test_missing_file_only_fails(tmp_path):
    res = run(tmp_path, [tmp_path / "ghost.jsonl"])
    assert res.status == "FAILED" and "MISSING" in res.metrics.failure_reason


def test_missing_file_plus_good_file_is_partial_failure(tmp_path):
    res = run(tmp_path, [tmp_path / "ghost.jsonl", write(tmp_path, "ok.jsonl", events("A", 3))])
    assert res.status == "PARTIAL_FAILURE" and res.metrics.inserted_records == 3 and res.exit_code == 1


def test_malformed_records_are_quarantined_not_dropped(tmp_path):
    lines = [jline(event_id="G1"), '{"broken":', "[1,2]", jline(event_id="G2", severity="DEBUG"), jline(event_id="G3")]
    res = run(tmp_path, [write(tmp_path, "m.jsonl", lines)])
    m = res.metrics
    assert (m.input_records, m.inserted_records, m.rejected_records) == (5, 2, 3)
    assert m.input_records == m.inserted_records + m.duplicate_records + m.rejected_records
    st = open_db(tmp_path)
    rows = st.conn.execute("SELECT primary_reason, raw_record, source_line FROM rejected_logs ORDER BY source_line").fetchall()
    assert [r[0] for r in rows] == ["PARSE_ERROR_MALFORMED_JSON", "UNEXPECTED_SCHEMA", "INVALID_SEVERITY"]
    assert rows[0][1] == '{"broken":' and [r[2] for r in rows] == [2, 3, 4]       # raw + line preserved


def test_invalid_reference_key_is_rejected(tmp_path):
    res = run(tmp_path, [write(tmp_path, "r.jsonl", [jline(event_id="R1", service_name="legacy-billing")])])
    assert res.metrics.rejected_by_reason == {"UNKNOWN_SERVICE": 1} and res.metrics.inserted_records == 0


def test_late_arriving_records_are_kept_and_flagged(tmp_path):
    run(tmp_path, [write(tmp_path, "now.jsonl", events("N", 10, start_min=120))])        # 12:00-12:09
    late_file = write(tmp_path, "late.jsonl", [jline(event_id=f"L{i}", timestamp=f"2026-11-05T08:0{i}:00Z") for i in range(3)])
    res = run(tmp_path, [late_file])
    assert res.metrics.inserted_records == 3 and res.metrics.late_records == 3
    st = open_db(tmp_path)
    assert st.conn.execute("SELECT COUNT(*) FROM processed_logs WHERE is_late=1").fetchone()[0] == 3
    # they land in their true (earlier) hour bucket, not the arrival time
    hours = {t["period"] for t in res.report["error_analytics"]["error_trend"]["hourly"]}
    assert "2026-11-05T08:00:00Z" in hours


def test_in_order_backfill_forward_is_not_late(tmp_path):
    run(tmp_path, [write(tmp_path, "a.jsonl", events("A", 5))])
    res = run(tmp_path, [write(tmp_path, "b.jsonl", events("B", 5, start_min=10))])
    assert res.metrics.late_records == 0


def test_out_of_order_records_within_a_file(tmp_path):
    lines = [jline(event_id="O1", timestamp="2026-11-05T10:05:00Z"), jline(event_id="O2", timestamp="2026-11-05T10:01:00Z"),
             jline(event_id="O3", timestamp="2026-11-05T10:09:00Z"), jline(event_id="O4", timestamp="2026-11-05T10:02:00Z")]
    res = run(tmp_path, [write(tmp_path, "o.jsonl", lines)])
    assert res.metrics.out_of_order_records == 2 and res.metrics.late_records == 0 and res.metrics.inserted_records == 4
    csv = (tmp_path / "out" / "processed_logs.csv").read_text().splitlines()
    assert [l.split(",")[0] for l in csv[1:]] == ["O2", "O4", "O1", "O3"]               # output sorted by event time


def test_partial_failure_rolls_back_the_failing_file_and_retry_recovers(tmp_path, monkeypatch):
    f1 = write(tmp_path, "f1.jsonl", events("A", 4))
    f2 = write(tmp_path, "f2.jsonl", events("B", 6, start_min=30))
    orig, calls = Storage.insert_processed_logs, {"n": 0}

    def flaky(self, records, run_id, mode="batch"):
        calls["n"] += 1
        if calls["n"] == 2:                                   # second file: write half, then blow up
            orig(self, list(records)[:3], run_id, mode)
            raise RuntimeError("disk exploded")
        return orig(self, records, run_id, mode)

    monkeypatch.setattr(Storage, "insert_processed_logs", flaky)
    res = run(tmp_path, [f1, f2])
    assert res.status == "PARTIAL_FAILURE" and res.metrics.files_failed == 1
    assert "f2.jsonl" in res.metrics.failure_reason and "disk exploded" in res.metrics.failure_reason
    st = open_db(tmp_path)
    assert st.count("processed_logs") == 4                    # f2's 3 half-written rows were rolled back
    assert st.conn.execute("SELECT status FROM processed_files WHERE file_name='f2.jsonl'").fetchone()[0] == "FAILED"
    st.close()

    monkeypatch.setattr(Storage, "insert_processed_logs", orig)  # "fix" the fault and simply re-run
    res2 = run(tmp_path, [f1, f2])
    assert res2.status == "SUCCESS" and res2.metrics.inserted_records == 6 and res2.metrics.duplicate_records == 4
    st = open_db(tmp_path)
    assert st.count("processed_logs") == 10
    assert st.conn.execute("SELECT status FROM processed_files WHERE file_name='f2.jsonl'").fetchone()[0] == "PROCESSED"


def test_transient_database_lock_is_retried(tmp_path, monkeypatch):
    orig, calls = Storage.insert_processed_logs, {"n": 0}

    def locked_once(self, records, run_id, mode="batch"):
        calls["n"] += 1
        if calls["n"] == 1:
            raise sqlite3.OperationalError("database is locked")
        return orig(self, records, run_id, mode)

    monkeypatch.setattr(Storage, "insert_processed_logs", locked_once)
    res = run(tmp_path, [write(tmp_path, "a.jsonl", events("A", 5))])
    assert res.status == "SUCCESS" and calls["n"] == 2 and res.metrics.inserted_records == 5


def test_non_transient_database_error_is_not_retried(tmp_path, monkeypatch):
    calls = {"n": 0}

    def broken(self, records, run_id, mode="batch"):
        calls["n"] += 1
        raise sqlite3.IntegrityError("constraint failed")

    monkeypatch.setattr(Storage, "insert_processed_logs", broken)
    res = run(tmp_path, [write(tmp_path, "a.jsonl", events("A", 2))])
    assert res.status == "FAILED" and calls["n"] == 1


def test_late_threshold_is_configurable(tmp_path):
    lines = [jline(event_id="T1", timestamp="2026-11-05T12:00:00Z"), jline(event_id="T2", timestamp="2026-11-05T11:00:00Z")]
    assert run(tmp_path, [write(tmp_path, "t.jsonl", lines)], db="a.db", late_threshold_minutes=30).metrics.late_records == 1
    assert run(tmp_path, [write(tmp_path, "t2.jsonl", lines)], db="b.db", late_threshold_minutes=120).metrics.late_records == 0


def _verify_dataset(tmp_path, name):
    spec = DATASETS[name]
    files = [spec["dir"] / spec["json"], spec["dir"] / spec["text"]]
    res = run(tmp_path, files, expected_path=spec["dir"] / spec["expected"])
    failed = [c for c in res.reconciliation["expected_results_checks"] if not c["passed"]]
    assert res.status == "SUCCESS" and failed == []
    assert res.reconciliation["internal_checks_passed"]
    return res


def test_sample_dataset_matches_independent_ground_truth(tmp_path):
    res = _verify_dataset(tmp_path, "sample")
    assert res.metrics.input_records == 100


def test_10k_dataset_matches_independent_ground_truth(tmp_path):
    res = _verify_dataset(tmp_path, "10k")
    assert res.metrics.input_records == 10_000 and res.metrics.inserted_records == 8771


def test_report_files_are_valid_json_with_required_sections(tmp_path):
    run(tmp_path, [write(tmp_path, "a.jsonl", events("A", 20))])
    rep = json.loads((tmp_path / "out" / "analytics_report.json").read_text())
    assert {"operational", "error_analytics", "latency_analytics", "database_totals"} <= set(rep)
    assert {"total_input_records", "duplicate_records", "rejected_records", "processing_seconds",
            "throughput_records_per_sec"} <= set(rep["operational"])
