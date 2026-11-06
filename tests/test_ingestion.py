from src.ingestion import discover_files, read_log_file
from tests.helpers import jline, write


def test_reads_json_file_with_line_numbers_and_checksum(tmp_path):
    p = write(tmp_path, "a.jsonl", [jline(event_id="1"), "", jline(event_id="2")])
    f = read_log_file(p)
    assert f.status == "OK" and len(f.records) == 2 and f.blank_lines == 1
    assert [r.line_number for r in f.records] == [1, 3]       # provenance survives blank lines
    assert all(r.source_file == "a.jsonl" and r.source_format == "json" for r in f.records)
    assert len(f.sha256) == 64 and f.sha256 == read_log_file(p).sha256


def test_text_file_format_by_extension(tmp_path):
    f = read_log_file(write(tmp_path, "a.log", ["2026-11-05 10:00:00 INFO [auth] hi"]))
    assert f.records[0].source_format == "text"


def test_unknown_extension_is_sniffed(tmp_path):
    f = read_log_file(write(tmp_path, "mix.dat", [jline(), "2026-11-05 10:00:00 INFO [auth] hi"]))
    assert [r.source_format for r in f.records] == ["json", "text"]


def test_missing_file_is_reported_not_raised(tmp_path):
    f = read_log_file(tmp_path / "nope.jsonl")
    assert f.status == "MISSING" and not f.readable


def test_empty_file(tmp_path):
    f = read_log_file(write(tmp_path, "e.jsonl", []))
    assert f.status == "EMPTY" and f.readable and f.records == []


def test_binary_and_directory_are_unreadable(tmp_path):
    (tmp_path / "bin.log").write_bytes(b"\x00\x01\x02binary")
    assert read_log_file(tmp_path / "bin.log").status == "UNREADABLE"
    assert read_log_file(tmp_path).status == "UNREADABLE"


def test_utf8_bom_and_crlf(tmp_path):
    p = tmp_path / "w.jsonl"
    p.write_bytes(b"\xef\xbb\xbf" + jline().encode() + b"\r\n" + jline(event_id="2").encode() + b"\r\n")
    f = read_log_file(p)
    assert len(f.records) == 2 and not f.records[0].raw.endswith("\r") and f.records[0].raw.startswith("{")


def test_discover_directory_skips_expected_results_and_dedups(tmp_path):
    write(tmp_path, "b.jsonl", [jline()]); write(tmp_path, "a.txt", ["x"])
    (tmp_path / "expected_results.json").write_text("{}")
    found = discover_files([tmp_path, tmp_path / "a.txt"])
    assert [p.name for p in found] == ["a.txt", "b.jsonl"]


def test_discover_glob(tmp_path):
    write(tmp_path, "x1.jsonl", [jline()]); write(tmp_path, "x2.jsonl", [jline()])
    assert len(discover_files([str(tmp_path / "x*.jsonl")])) == 2
