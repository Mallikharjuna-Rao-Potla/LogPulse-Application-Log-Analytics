"""Shared test helpers (plain functions - no hidden fixtures)."""
import json
from datetime import datetime, timedelta

from src.ingestion import RawRecord
from src.parser import parse_record
from src.pipeline import Pipeline, PipelineConfig
from src.storage import Storage
from src.transformation import derive_fields, normalize_record

DEFAULT = dict(event_id="E1", timestamp="2026-11-05T10:00:00Z", severity="INFO", service_name="auth",
               message="User login successful", latency_ms=120)


def jline(drop=(), **over) -> str:
    doc = {**DEFAULT, **over}
    for key in drop:
        doc.pop(key, None)
    return json.dumps(doc)


def raw(line, fmt="json", n=1):
    return RawRecord("test.jsonl" if fmt == "json" else "test.txt", "sha-test", n, line, fmt)


def norm(line, fmt="json"):
    return normalize_record(parse_record(raw(line, fmt)))


def valid_rec(run_id="r1", **over):
    rec = derive_fields(norm(jline(**over)))
    rec["run_id"] = run_id
    return rec


def write(tmp_path, name, lines):
    p = tmp_path / name
    p.write_text("\n".join(lines) + ("\n" if lines else ""), encoding="utf-8")
    return p


def run(tmp_path, files, db="t.db", **kw):
    cfg = PipelineConfig(input_paths=[str(f) for f in files], db_path=str(tmp_path / db),
                         output_dir=str(tmp_path / "out"), charts=False, retry_delay=0, **kw)
    pipe = Pipeline(cfg)
    try:
        return pipe.run()
    finally:
        pipe.storage.close()


def open_db(tmp_path, db="t.db"):
    return Storage(tmp_path / db)


def events(prefix, n, start_min=0, **over):
    """n distinct valid JSON events, one per minute starting at 10:00 + start_min."""
    base = datetime(2026, 11, 5, 10, 0, 0)
    return [jline(event_id=f"{prefix}{i}",
                  timestamp=(base + timedelta(minutes=start_min + i)).strftime("%Y-%m-%dT%H:%M:%SZ"), **over)
            for i in range(n)]
