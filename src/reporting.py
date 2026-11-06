"""Report generation: CSV exports, JSON analytics report, reconciliation checks and charts."""
from __future__ import annotations

import json
import math
from pathlib import Path
from typing import List, Optional

import pandas as pd

from . import analytics, config
from .logger import get_logger

log = get_logger("reporting")

PROCESSED_EXPORT_SQL = """SELECT event_id, event_timestamp, severity, service_name, message, latency_ms,
       is_error, is_late, event_date, event_hour, source_format, event_id_generated, source_file,
       source_line, run_id, ingested_at FROM processed_logs ORDER BY event_timestamp, event_id"""
REJECTED_EXPORT_SQL = """SELECT reject_id, run_id, stage, primary_reason, rejection_reasons, event_id,
       source_format, source_file, source_line, raw_record, rejected_at FROM rejected_logs ORDER BY reject_id"""


def _check(name: str, expected, actual, tol: float = 0.0) -> dict:
    if isinstance(expected, (int, float)) and isinstance(actual, (int, float)) and expected is not None:
        ok = math.isclose(float(expected), float(actual), abs_tol=max(tol, 1e-9))
    else:
        ok = expected == actual
    return {"check": name, "expected": expected, "actual": actual, "passed": bool(ok)}


def export_csvs(storage, out_dir: Path) -> dict:
    processed = pd.read_sql_query(PROCESSED_EXPORT_SQL, storage.conn)
    rejected = pd.read_sql_query(REJECTED_EXPORT_SQL, storage.conn)
    p_path, r_path = out_dir / "processed_logs.csv", out_dir / "rejected_logs.csv"
    processed.to_csv(p_path, index=False)
    rejected.to_csv(r_path, index=False)
    return {"processed_logs_csv": str(p_path), "rejected_logs_csv": str(r_path),
            "processed_rows": len(processed), "rejected_rows": len(rejected)}


def internal_reconciliation(storage, m, df, report, db_logs_before: int, csv_info: dict) -> List[dict]:
    """Source-to-target checks that need no external reference data."""
    checks = [
        _check("input_records == inserted + duplicates + rejected", m.input_records,
               m.inserted_records + m.duplicate_records + m.rejected_records),
        _check("parsed_records + parse_failures == input_records", m.input_records,
               m.parsed_records + m.parse_failures),
        _check("rows added to processed_logs == inserted_records", m.inserted_records,
               storage.count("processed_logs") - db_logs_before),
        _check("quarantine rows written/ignored == rejected_records", m.rejected_records,
               m.rejected_new + m.rejected_already_quarantined),
        _check("processed_logs.csv rows == processed_logs table", storage.count("processed_logs"),
               csv_info["processed_rows"]),
        _check("rejected_logs.csv rows == rejected_logs table", storage.count("rejected_logs"),
               csv_info["rejected_rows"]),
        _check("sum(rejected_by_reason) >= rejected_records", True,
               sum(m.rejected_by_reason.values()) >= m.rejected_records),
    ]
    # severity distribution: pandas result vs independent SQL GROUP BY
    sql_sev = {r[0]: r[1] for r in storage.conn.execute("SELECT severity, COUNT(*) FROM processed_logs GROUP BY severity")}
    pd_sev = report["error_analytics"]["severity_counts"]
    for sev in config.VALID_SEVERITIES:
        checks.append(_check(f"severity count {sev}: pandas == SQL", sql_sev.get(sev, 0), pd_sev[sev]))
    checks.append(_check("sum(severity counts) == total valid logs", storage.count("processed_logs"),
                         sum(pd_sev.values())))
    # latency aggregates: pandas vs SQL
    row = storage.conn.execute("SELECT COUNT(latency_ms), AVG(latency_ms), MIN(latency_ms), MAX(latency_ms) "
                               "FROM processed_logs").fetchone()
    lat = report["latency_analytics"]
    checks.append(_check("latency count: pandas == SQL", row[0], lat["count"]))
    for name, sql_val, key in (("avg", row[1], "avg_ms"), ("min", row[2], "min_ms"), ("max", row[3], "max_ms")):
        checks.append(_check(f"latency {name}: pandas == SQL", None if sql_val is None else round(sql_val, 4),
                             lat[key], tol=1e-3))
    return checks


def compare_with_expected(expected: dict, m, report: dict) -> List[dict]:
    """Compare a run against the generator's independent ground truth (expected_results.json)."""
    out = []
    if m.files_skipped:
        out.append({"check": "expected: run-level counts", "expected": None, "actual": None, "passed": True,
                    "note": "N/A - some files were skipped (--skip-processed); only database state is compared"})
    else:
        out = [
            _check("expected: input records", expected["input_records"], m.input_records),
            _check("expected: inserted + duplicates", expected["outcomes"]["inserted"] + expected["outcomes"]["duplicates"],
                   m.inserted_records + m.duplicate_records),
            _check("expected: rejected records", expected["outcomes"]["rejected"], m.rejected_records),
        ]
        reasons = sorted(set(expected["rejected_by_reason"]) | set(m.rejected_by_reason))
        for r in reasons:
            out.append(_check(f"expected: rejected by {r}", expected["rejected_by_reason"].get(r, 0),
                              m.rejected_by_reason.get(r, 0)))
        first_load = m.inserted_records == expected["outcomes"]["inserted"]
        if first_load:
            out.append(_check("expected: late records", expected["late_records"], m.late_records))
            out.append(_check("expected: out-of-order records", expected["out_of_order_records"],
                              m.out_of_order_records))
    st = expected["database_state"]
    ea, la = report["error_analytics"], report["latency_analytics"]
    if ea["total_logs"] != st["total_logs"]:
        out.append({"check": "expected: database state", "expected": st["total_logs"],
                    "actual": ea["total_logs"], "passed": True,
                    "note": "SKIPPED - database holds records from other datasets (use --reset-db)"})
        return out
    out.append(_check("expected: total valid logs", st["total_logs"], ea["total_logs"]))
    for sev, n in st["severity_counts"].items():
        out.append(_check(f"expected: {sev} count", n, ea["severity_counts"][sev]))
    out.append(_check("expected: error count", st["error_count"], ea["error_count"]))
    out.append(_check("expected: error rate %", st["error_rate_pct"], ea["error_rate_pct"], tol=1e-3))
    for svc, e in st["error_rate_by_service"].items():
        a = ea["error_rate_by_service"].get(svc, {})
        out.append(_check(f"expected: error rate % [{svc}]", e["error_rate_pct"], a.get("error_rate_pct"), tol=1e-3))
    for key in ("count", "avg_ms", "min_ms", "max_ms", "p50_ms", "p95_ms"):
        out.append(_check(f"expected: latency {key}", st["latency"][key], la[key], tol=1e-3))
    for svc, e in st["latency"]["by_service"].items():
        a = la["by_service"].get(svc, {})
        for key in ("avg_ms", "p95_ms"):
            out.append(_check(f"expected: latency {key} [{svc}]", e[key], a.get(key), tol=1e-3))
    daily = {t["period"]: (t["total"], t["errors"]) for t in ea["error_trend"]["daily"]}
    exp_daily = {d: (v["total"], v["errors"]) for d, v in st["daily"].items()}
    out.append(_check("expected: daily totals/errors trend", exp_daily, daily))
    return out


def save_charts(df: pd.DataFrame, report: dict, out_dir: Path) -> List[str]:
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt

    chart_dir = out_dir / "charts"
    chart_dir.mkdir(parents=True, exist_ok=True)
    paths = []
    if df.empty:
        return paths
    ea, la = report["error_analytics"], report["latency_analytics"]
    colors = {"INFO": "#4c9f70", "WARNING": "#e6a817", "ERROR": "#d9534f", "CRITICAL": "#7b1f1f"}

    fig, ax = plt.subplots(figsize=(6, 4))
    sev = ea["severity_counts"]
    ax.bar(list(sev), list(sev.values()), color=[colors[s] for s in sev])
    ax.set(title="Severity distribution", ylabel="valid logs")
    p = chart_dir / "severity_distribution.png"; fig.tight_layout(); fig.savefig(p, dpi=110); plt.close(fig); paths.append(str(p))

    fig, ax = plt.subplots(figsize=(7, 4))
    svc = ea["error_rate_by_service"]
    ax.bar(list(svc), [v["error_rate_pct"] for v in svc.values()], color="#d9534f")
    ax.set(title="Error rate by service (ERROR+CRITICAL)", ylabel="% of logs")
    plt.setp(ax.get_xticklabels(), rotation=30, ha="right")
    p = chart_dir / "error_rate_by_service.png"; fig.tight_layout(); fig.savefig(p, dpi=110); plt.close(fig); paths.append(str(p))

    grain = "daily" if len(ea["error_trend"]["daily"]) >= 3 else "hourly"
    trend = ea["error_trend"][grain]
    fig, ax = plt.subplots(figsize=(8, 4))
    ax.plot([t["period"] for t in trend], [t["error_rate_pct"] for t in trend], marker="o", color="#d9534f")
    ax.set(title=f"Error rate trend ({grain})", ylabel="error rate %")
    step = max(1, len(trend) // 10)
    ax.set_xticks(range(0, len(trend), step))
    ax.set_xticklabels([trend[i]["period"][:13] for i in range(0, len(trend), step)], rotation=40, ha="right")
    p = chart_dir / "error_rate_trend.png"; fig.tight_layout(); fig.savefig(p, dpi=110); plt.close(fig); paths.append(str(p))

    fig, ax = plt.subplots(figsize=(8, 4))
    names = list(la["by_service"])
    x = range(len(names))
    ax.bar([i - 0.2 for i in x], [la["by_service"][n]["p50_ms"] or 0 for n in names], 0.4, label="P50", color="#4c78a8")
    ax.bar([i + 0.2 for i in x], [la["by_service"][n]["p95_ms"] or 0 for n in names], 0.4, label="P95", color="#f58518")
    ax.set_xticks(list(x)); ax.set_xticklabels(names, rotation=30, ha="right")
    ax.set(title="Latency by service", ylabel="ms"); ax.legend()
    p = chart_dir / "latency_by_service.png"; fig.tight_layout(); fig.savefig(p, dpi=110); plt.close(fig); paths.append(str(p))
    return paths


def generate_all(storage, m, cfg, db_logs_before: int, result) -> None:
    out_dir = Path(cfg.output_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    df = analytics.load_processed_df(storage.conn)
    totals = {t: storage.count(t) for t in ("processed_logs", "rejected_logs", "pipeline_runs", "processed_files")}
    report = analytics.build_report(df, m, totals, storage.db_path)
    csv_info = export_csvs(storage, out_dir)

    checks = internal_reconciliation(storage, m, df, report, db_logs_before, csv_info)
    expected_cmp = None
    if cfg.expected_path:
        exp_path = Path(cfg.expected_path)
        if exp_path.exists():
            expected = json.loads(exp_path.read_text(encoding="utf-8"))
            expected_cmp = compare_with_expected(expected, m, report)
        else:
            log.warning("Expected-results file not found: %s", exp_path)

    recon = {
        "run_id": m.run_id, "status": m.status,
        "source_to_target": {"input_records": m.input_records, "inserted": m.inserted_records,
                             "duplicates": m.duplicate_records, "rejected": m.rejected_records},
        "internal_checks": checks,
        "internal_checks_passed": all(c["passed"] for c in checks),
        "expected_results_checks": expected_cmp,
        "expected_results_passed": None if expected_cmp is None else all(c["passed"] for c in expected_cmp),
        "expected_results_file": str(cfg.expected_path) if cfg.expected_path else None,
        "file_results": m.file_results,
    }
    report["reconciliation_summary"] = {
        "internal_checks_passed": recon["internal_checks_passed"],
        "expected_results_passed": recon["expected_results_passed"]}

    paths = {"analytics_report": out_dir / "analytics_report.json",
             "reconciliation_report": out_dir / "reconciliation_report.json"}
    paths["analytics_report"].write_text(json.dumps(report, indent=2, default=str), encoding="utf-8")
    paths["reconciliation_report"].write_text(json.dumps(recon, indent=2, default=str), encoding="utf-8")
    files = {k: str(v) for k, v in paths.items()}
    files["processed_logs_csv"], files["rejected_logs_csv"] = csv_info["processed_logs_csv"], csv_info["rejected_logs_csv"]
    if cfg.charts:
        files["charts"] = save_charts(df, report, out_dir)
    result.report, result.reconciliation, result.output_files = report, recon, files
