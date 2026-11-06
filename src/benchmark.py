"""Performance benchmark: 100 / 10,000 / 100,000 records, before vs after the batching optimisation.

    python -m src.benchmark                 # all sizes, both modes
    python -m src.benchmark --sizes sample 10k

Each (size, mode) case runs in a fresh subprocess against a throw-away database, so timings are not
affected by earlier cases and peak memory (RSS) is measured per case.
  baseline   : per-record SELECT + INSERT, normalisation fast paths OFF
  batched_db : chunked IN(...) lookups + executemany() inserts, fast paths OFF   (suggested optimisation)
  optimised  : batched DB + normalisation fast paths ON                          (final configuration)
Profiling showed normalisation (not SQLite) dominates the runtime, hence the second optimisation.
"""
from __future__ import annotations

import argparse
import json
import subprocess
import sys
import tempfile
from pathlib import Path

from . import config, generator

MODES = {"baseline": ("row", False), "batched_db": ("batch", False), "optimised": ("batch", True)}
SIZES = {"sample": 100, "10k": 10_000, "100k": 100_000}


def _peak_rss_mb():
    try:
        import resource
        peak = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
        return round(peak / (1024 * 1024 if sys.platform == "darwin" else 1024), 1)
    except Exception:       # e.g. Windows
        return None


def worker(size: str, mode: str) -> dict:
    from .pipeline import PipelineConfig, run_pipeline
    write_mode, fast = MODES[mode]
    config.FAST_PATHS = fast
    spec = generator.DATASETS[size]
    files = [Path(spec["dir"]) / spec["json"], Path(spec["dir"]) / spec["text"]]
    if not all(f.exists() for f in files):
        generator.write_dataset(size)
    with tempfile.TemporaryDirectory() as tmp:
        cfg = PipelineConfig(input_paths=[str(f) for f in files], db_path=str(Path(tmp) / "bench.db"),
                             output_dir=tmp, write_reports=False, write_mode=write_mode)
        m = run_pipeline(cfg).metrics
    return {"dataset": size, "mode": mode, "records": m.input_records, "inserted": m.inserted_records,
            "duplicates": m.duplicate_records, "rejected": m.rejected_records,
            "processing_seconds": round(m.processing_seconds, 3), "records_per_second": round(m.throughput_rps),
            "peak_rss_mb": _peak_rss_mb(), "status": m.status}


def main(argv=None) -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--sizes", nargs="+", choices=list(SIZES), default=list(SIZES))
    ap.add_argument("--worker", nargs=2, metavar=("SIZE", "MODE"), help=argparse.SUPPRESS)
    args = ap.parse_args(argv)
    if args.worker:
        print("RESULT " + json.dumps(worker(*args.worker)))
        return

    results = []
    for size in args.sizes:
        for mode in MODES:
            proc = subprocess.run([sys.executable, "-m", "src.benchmark", "--worker", size, mode],
                                  capture_output=True, text=True, cwd=config.PROJECT_ROOT)
            line = next((l for l in proc.stdout.splitlines() if l.startswith("RESULT ")), None)
            if not line:
                raise SystemExit(f"benchmark case {size}/{mode} failed:\n{proc.stdout}\n{proc.stderr}")
            results.append(json.loads(line[7:]))
            r = results[-1]
            print(f"{size:>6} {mode:>6}: {r['processing_seconds']:>7.3f}s  {r['records_per_second']:>7,} rec/s  "
                  f"peak RSS {r['peak_rss_mb']} MB")

    # before/after summary
    md = ["| Dataset | Records | Baseline | + batched DB writes | + normalisation fast paths (final) | Total speed-up | Peak RSS (final) |",
          "|---|---:|---:|---:|---:|---:|---:|"]
    for size in args.sizes:
        r = {mode: next(x for x in results if x["dataset"] == size and x["mode"] == mode) for mode in MODES}
        cell = lambda x: f"{x['processing_seconds']}s ({x['records_per_second']:,}/s)"
        md.append(f"| {size} | {r['optimised']['records']:,} | {cell(r['baseline'])} | {cell(r['batched_db'])} | "
                  f"{cell(r['optimised'])} | {r['baseline']['processing_seconds'] / r['optimised']['processing_seconds']:.2f}x | "
                  f"{r['optimised']['peak_rss_mb']} MB |")
    config.OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    (config.OUTPUT_DIR / "benchmark_results.json").write_text(json.dumps(results, indent=2), encoding="utf-8")
    (config.OUTPUT_DIR / "benchmark_results.md").write_text("\n".join(md) + "\n", encoding="utf-8")
    print("\n" + "\n".join(md))


if __name__ == "__main__":
    main()
