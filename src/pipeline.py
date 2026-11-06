"""Pipeline orchestration:

INGESTION -> PARSING -> NORMALISATION -> VALIDATION -> DEDUPLICATION -> STORAGE -> ANALYTICS -> REPORTS

Each input file is one atomic unit of work (single DB transaction). A failing file is rolled back and
reported; remaining files still run -> PARTIAL_FAILURE. Because writes are idempotent
(event_id PK + INSERT OR IGNORE), re-running after a failure is always safe.
"""
from __future__ import annotations

import sqlite3
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import List, Optional

from . import analytics, config, reporting
from .ingestion import IngestedFile, discover_files, read_log_file
from .logger import (RunMetrics, STATUS_FAILED, STATUS_PARTIAL, STATUS_SUCCESS, get_logger, utc_now_iso)
from .parser import parse_record
from .storage import Storage, is_transient
from .transformation import derive_fields, flag_late_and_out_of_order, normalize_record
from .validation import (DUPLICATE_EVENT_ID_CONFLICT, build_quarantine_record, detect_duplicates,
                         split_valid_invalid)

log = get_logger("pipeline")


@dataclass
class PipelineConfig:
    input_paths: List = field(default_factory=list)
    db_path: object = config.DB_PATH
    output_dir: object = config.OUTPUT_DIR
    expected_path: Optional[object] = None
    reset_db: bool = False
    skip_processed: bool = False          # skip files whose checksum was already PROCESSED
    write_reports: bool = True
    charts: bool = True
    write_mode: str = "batch"             # 'batch' (executemany) | 'row' (naive baseline)
    late_threshold_minutes: float = config.LATE_THRESHOLD_MINUTES
    retry_attempts: int = config.DB_RETRY_ATTEMPTS
    retry_delay: float = config.DB_RETRY_DELAY_SECONDS


@dataclass
class RunResult:
    metrics: RunMetrics
    report: Optional[dict] = None
    reconciliation: Optional[dict] = None
    output_files: dict = field(default_factory=dict)

    @property
    def status(self) -> str:
        return self.metrics.status

    @property
    def exit_code(self) -> int:
        return {STATUS_SUCCESS: 0, STATUS_PARTIAL: 1}.get(self.metrics.status, 2)


class Pipeline:
    def __init__(self, cfg: PipelineConfig, storage: Optional[Storage] = None):
        self.cfg = cfg
        if storage is None:
            if cfg.reset_db and str(cfg.db_path) != ":memory:":
                for suffix in ("", "-wal", "-shm"):
                    Path(str(cfg.db_path) + suffix).unlink(missing_ok=True)
            storage = Storage(cfg.db_path)
        self.storage = storage

    # ------------------------------------------------------------------------------------
    def run(self) -> RunResult:
        cfg, st = self.cfg, self.storage
        m = RunMetrics()
        t_start = time.perf_counter()
        st.start_run(m.run_id, m.started_at)
        log.info("Run %s started", m.run_id)

        files = discover_files(cfg.input_paths)
        m.files_total = len(files)
        if not files:
            m.add_failure("No input files found")
            log.error("No input files found for %s", cfg.input_paths)

        known_services = st.get_known_services()
        watermark = st.max_event_epoch()
        db_logs_before = st.count("processed_logs")
        t_proc = time.perf_counter()

        for path in files:
            self._process_file(path, m, known_services, watermark)

        m.processing_seconds = time.perf_counter() - t_proc
        m.status = self._final_status(m)
        m.ended_at = utc_now_iso()
        m.duration_seconds = time.perf_counter() - t_start
        st.finish_run(m)

        result = RunResult(metrics=m)
        if cfg.write_reports:
            try:
                reporting.generate_all(st, m, cfg, db_logs_before, result)
            except Exception as exc:  # report problems must not hide the processing outcome
                log.exception("Report generation failed")
                m.add_failure(f"report generation failed: {exc}")
                if m.status == STATUS_SUCCESS:
                    m.status = STATUS_PARTIAL
            m.duration_seconds = time.perf_counter() - t_start
            m.ended_at = utc_now_iso()
            st.finish_run(m)
        log.info("Run %s finished: %s input=%d inserted=%d duplicates=%d rejected=%d (%.2fs, %.0f rec/s)",
                 m.run_id, m.status, m.input_records, m.inserted_records, m.duplicate_records,
                 m.rejected_records, m.processing_seconds, m.throughput_rps)
        return result

    # ------------------------------------------------------------------------------------
    @staticmethod
    def _final_status(m: RunMetrics) -> str:
        if m.files_total == 0 or (m.files_failed > 0 and m.files_processed == 0 and m.files_skipped == 0):
            return STATUS_FAILED
        return STATUS_PARTIAL if m.files_failed > 0 else STATUS_SUCCESS

    def _process_file(self, path, m: RunMetrics, known_services, watermark) -> None:
        cfg, st = self.cfg, self.storage
        ing: IngestedFile = read_log_file(path)
        if not ing.readable:
            m.files_failed += 1
            m.add_failure(f"{ing.name}: {ing.status} - {ing.error}")
            m.file_results.append({"file": ing.name, "status": ing.status, "error": ing.error})
            log.error("Cannot read %s: %s (%s)", ing.name, ing.status, ing.error)
            return

        previous = st.get_processed_file(ing.sha256)
        if previous is not None and previous["status"] == "PROCESSED":
            if cfg.skip_processed:
                m.files_skipped += 1
                m.file_results.append({"file": ing.name, "status": "SKIPPED_ALREADY_PROCESSED"})
                log.info("Skipping %s (checksum already processed in run %s)", ing.name, previous["first_run_id"])
                return
            log.warning("%s was already processed (checksum match): records will reconcile as duplicates",
                        ing.name)

        # ---- pure, in-memory stages: parse -> normalise -> validate ---------------------
        run_id = m.run_id
        rejected, normalised, parse_fail = [], [], 0
        for raw in ing.records:
            parsed = parse_record(raw)
            if parsed.ok:
                normalised.append(normalize_record(parsed))
            else:
                parse_fail += 1
                rejected.append(build_quarantine_record(raw, [parsed.error_code], "PARSING", run_id))
        valid, invalid = split_valid_invalid(normalised, known_services)
        for rec, reasons in invalid:
            rejected.append(build_quarantine_record(rec["_raw"], reasons, "VALIDATION", run_id, rec["event_id"]))
        validation_fail = len(invalid)

        # ---- DB stages: dedupe -> flag late -> write (one atomic transaction, retried if locked)
        outcome = None
        for attempt in range(1, cfg.retry_attempts + 1):
            try:
                outcome = self._persist(ing, valid, rejected, run_id, watermark)
                break
            except sqlite3.Error as exc:
                if is_transient(exc) and attempt < cfg.retry_attempts:
                    log.warning("Transient DB error on %s (attempt %d/%d): %s", ing.name, attempt,
                                cfg.retry_attempts, exc)
                    time.sleep(cfg.retry_delay * attempt)
                    continue
                self._fail_file(ing, m, exc)
                return
            except Exception as exc:
                self._fail_file(ing, m, exc)
                return

        # ---- success: merge counters ----------------------------------------------------
        m.files_processed += 1
        m.blank_lines_skipped += ing.blank_lines
        m.input_records += len(ing.records)
        m.parsed_records += len(ing.records) - parse_fail
        m.parse_failures += parse_fail
        m.validation_failures += validation_fail
        m.conflict_rejections += outcome["conflicts"]
        m.duplicate_records += outcome["duplicates"]
        m.inserted_records += outcome["inserted"]
        m.late_records += outcome["late"]
        m.out_of_order_records += outcome["ooo"]
        m.rejected_new += outcome["rejected_new"]
        m.rejected_already_quarantined += outcome["rejected_existing"]
        for rej in outcome["all_rejected"]:
            for reason in rej["rejection_reasons"].split(";"):
                m.rejected_by_reason[reason] = m.rejected_by_reason.get(reason, 0) + 1
        m.file_results.append({
            "file": ing.name, "status": "PROCESSED", "sha256": ing.sha256, "records": len(ing.records),
            "inserted": outcome["inserted"], "duplicates": outcome["duplicates"],
            "rejected": len(outcome["all_rejected"]), "already_seen_checksum": previous is not None})
        log.info("%s: records=%d inserted=%d duplicates=%d rejected=%d", ing.name, len(ing.records),
                 outcome["inserted"], outcome["duplicates"], len(outcome["all_rejected"]))

    def _persist(self, ing, valid, rejected, run_id, watermark) -> dict:
        cfg, st = self.cfg, self.storage
        with st.transaction():
            existing = st.fetch_existing_hashes([r["event_id"] for r in valid], cfg.write_mode)
            new, dups, conflicts = detect_duplicates(valid, existing)
            all_rejected = list(rejected)
            for rec in conflicts:
                all_rejected.append(build_quarantine_record(
                    rec["_raw"], [DUPLICATE_EVENT_ID_CONFLICT], "VALIDATION", run_id, rec["event_id"]))
            late, ooo = flag_late_and_out_of_order(new, watermark, cfg.late_threshold_minutes)
            for rec in new:
                derive_fields(rec)
                rec["run_id"] = run_id
            inserted = st.insert_processed_logs(new, run_id, cfg.write_mode)
            race_dups = len(new) - inserted          # lost a race to another writer -> still a duplicate
            rej_new, rej_existing = st.insert_rejected_logs(all_rejected)
            st.upsert_processed_file(sha256=ing.sha256, file_name=ing.name, file_path=ing.path,
                                     size_bytes=ing.size_bytes, record_count=len(ing.records),
                                     status="PROCESSED", run_id=run_id)
        return {"inserted": inserted, "duplicates": len(dups) + race_dups, "conflicts": len(conflicts),
                "late": late, "ooo": ooo, "all_rejected": all_rejected,
                "rejected_new": rej_new, "rejected_existing": rej_existing}

    def _fail_file(self, ing, m: RunMetrics, exc: Exception) -> None:
        """The file's transaction was rolled back; record the failure (own small transaction)."""
        reason = f"{ing.name}: {type(exc).__name__}: {exc}"
        log.error("Processing failed for %s - rolled back. %s", ing.name, reason)
        m.files_failed += 1
        m.add_failure(reason)
        m.file_results.append({"file": ing.name, "status": "FAILED", "error": reason})
        try:
            with self.storage.transaction():
                self.storage.upsert_processed_file(
                    sha256=ing.sha256, file_name=ing.name, file_path=ing.path, size_bytes=ing.size_bytes,
                    record_count=len(ing.records), status="FAILED", run_id=m.run_id, last_error=str(exc)[:500])
        except Exception:
            log.exception("Could not record failed file %s", ing.name)


def run_pipeline(cfg: PipelineConfig) -> RunResult:
    pipe = Pipeline(cfg)
    try:
        return pipe.run()
    finally:
        pipe.storage.close()
