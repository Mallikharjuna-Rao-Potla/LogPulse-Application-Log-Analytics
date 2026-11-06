#!/usr/bin/env python3
"""LogPulse command line entry point.

Examples
    python run_pipeline.py                       # process the bundled sample dataset
    python run_pipeline.py --dataset 10k         # 10,000-record dataset (+ verify against ground truth)
    python run_pipeline.py --dataset 100k --reset-db
    python run_pipeline.py --input my_logs/ other.log --db my.db
Exit code: 0 = SUCCESS, 1 = PARTIAL_FAILURE, 2 = FAILED
"""
from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent
sys.path.insert(0, str(ROOT))

from src import config, generator                      # noqa: E402
from src.logger import configure_logging               # noqa: E402
from src.pipeline import PipelineConfig, run_pipeline  # noqa: E402


def build_parser() -> argparse.ArgumentParser:
    ap = argparse.ArgumentParser(description="LogPulse - application log processing & analytics pipeline")
    ap.add_argument("--dataset", choices=list(generator.DATASETS), help="bundled dataset to process (default: sample)")
    ap.add_argument("--input", nargs="+", help="log files / directories / globs (overrides --dataset)")
    ap.add_argument("--db", default=str(config.DB_PATH), help="SQLite database path")
    ap.add_argument("--output-dir", default=str(config.OUTPUT_DIR), help="where CSV/JSON/chart outputs go")
    ap.add_argument("--expected", help="expected_results.json to verify against (auto for --dataset)")
    ap.add_argument("--reset-db", action="store_true", help="delete the database first (clean slate)")
    ap.add_argument("--skip-processed", action="store_true",
                    help="skip files whose checksum was already processed (default: re-read and count as duplicates)")
    ap.add_argument("--write-mode", choices=["batch", "row"], default="batch", help="'row' = slow baseline")
    ap.add_argument("--late-threshold-minutes", type=float, default=config.LATE_THRESHOLD_MINUTES)
    ap.add_argument("--no-charts", action="store_true")
    ap.add_argument("-v", "--verbose", action="store_true")
    ap.add_argument("-q", "--quiet", action="store_true", help="only warnings/errors on the console")
    return ap


def resolve_inputs(args):
    if args.input:
        return args.input, args.expected
    name = args.dataset or "sample"
    spec = generator.DATASETS[name]
    files = [Path(spec["dir"]) / spec["json"], Path(spec["dir"]) / spec["text"]]
    if not all(f.exists() for f in files):
        print(f"Dataset '{name}' not found - generating it (seed {config.SEED})...")
        generator.write_dataset(name)
    return [str(f) for f in files], args.expected or str(Path(spec["dir"]) / spec["expected"])


def main(argv=None) -> int:
    args = build_parser().parse_args(argv)
    configure_logging(logging.DEBUG if args.verbose else logging.WARNING if args.quiet else logging.INFO)
    inputs, expected = resolve_inputs(args)
    cfg = PipelineConfig(input_paths=inputs, db_path=args.db, output_dir=args.output_dir, expected_path=expected,
                         reset_db=args.reset_db, skip_processed=args.skip_processed, charts=not args.no_charts,
                         write_mode=args.write_mode, late_threshold_minutes=args.late_threshold_minutes)
    result = run_pipeline(cfg)
    m = result.metrics
    line = "=" * 64
    print(f"\n{line}\n LogPulse run {m.run_id}  ->  {m.status}\n{line}")
    print(f" Files             : {m.files_processed} processed, {m.files_failed} failed, {m.files_skipped} skipped")
    print(f" Input records     : {m.input_records:,}")
    print(f" Inserted          : {m.inserted_records:,}")
    print(f" Duplicates        : {m.duplicate_records:,}")
    print(f" Rejected          : {m.rejected_records:,}   (quarantined; see rejected_logs.csv)")
    print(f" Late / out-of-order: {m.late_records:,} / {m.out_of_order_records:,}")
    print(f" Processing time   : {m.processing_seconds:.2f}s  ({m.throughput_rps:,.0f} records/s)")
    if m.failure_reason:
        print(f" FAILURE REASON    : {m.failure_reason}")
    if result.report:
        ea, la = result.report["error_analytics"], result.report["latency_analytics"]
        print(f" Valid logs in DB  : {ea['total_logs']:,}   error rate {ea['error_rate_pct']}%   "
              f"avg latency {la['avg_ms']} ms   P95 {la['p95_ms']} ms")
        rc = result.reconciliation
        print(f" Reconciliation    : internal checks {'PASS' if rc['internal_checks_passed'] else 'FAIL'}"
              f" ({sum(c['passed'] for c in rc['internal_checks'])}/{len(rc['internal_checks'])})")
        if rc["expected_results_checks"] is not None:
            ok = sum(c['passed'] for c in rc['expected_results_checks'])
            print(f" Expected results  : {'PASS' if rc['expected_results_passed'] else 'FAIL'}"
                  f" ({ok}/{len(rc['expected_results_checks'])} checks vs ground truth)")
        print(f" Outputs in        : {args.output_dir}")
    print(line)
    return result.exit_code


if __name__ == "__main__":
    sys.exit(main())
