import json

from src import generator
from src.generator import DATASETS, generate_dataset


def test_generation_is_reproducible_with_fixed_seed():
    assert generate_dataset("sample") == generate_dataset("sample")


def test_datasets_differ_from_each_other():
    assert generate_dataset("sample")[0] != generate_dataset("10k")[0][:75]


def test_record_counts_and_expected_invariants():
    j, t, exp = generate_dataset("sample")
    assert len(j) + len(t) == 100 == exp["input_records"]
    out = exp["outcomes"]
    assert out["inserted"] + out["duplicates"] + out["rejected"] == 100
    assert exp["database_state"]["total_logs"] == out["inserted"]


def test_every_defect_category_is_present_in_the_sample():
    _, _, exp = generate_dataset("sample")
    assert set(exp["rejected_by_reason"]) >= {
        "MISSING_EVENT_ID", "MISSING_SERVICE_NAME", "MISSING_MESSAGE", "MISSING_SEVERITY", "MISSING_TIMESTAMP",
        "INVALID_TIMESTAMP", "INVALID_SEVERITY", "NEGATIVE_LATENCY", "INVALID_LATENCY_TYPE", "LATENCY_EXCEEDS_MAX",
        "UNKNOWN_SERVICE", "INVALID_EVENT_ID", "UNEXPECTED_SCHEMA"}
    assert exp["outcomes"]["duplicates"] >= 1 and exp["late_records"] >= 1 and exp["out_of_order_records"] >= 1


def test_all_severities_and_both_formats_generated():
    j, t, exp = generate_dataset("sample")
    assert all(v > 0 for v in exp["database_state"]["severity_counts"].values())
    assert j and t


def test_write_dataset_creates_files(tmp_path):
    paths = generator.write_dataset("sample", tmp_path)
    assert all(p.exists() and p.stat().st_size > 0 for p in paths.values())
    assert json.loads(paths["expected"].read_text())["dataset"] == "sample"


def test_percentile_helper_matches_known_values():
    assert generator.percentile(list(range(1, 101)), 95) == 95.05
    assert generator.percentile([], 50) is None and generator.percentile([7], 95) == 7
