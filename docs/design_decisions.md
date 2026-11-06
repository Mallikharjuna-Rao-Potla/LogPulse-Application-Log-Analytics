# Design decisions (and how to explain them)

**Why SQLite?** Zero-setup, transactional (ACID), supports constraints/FKs/indexes/upserts, and ships with Python – the pipeline runs after `pip install`.
Limits: single writer; the storage layer is isolated so Postgres is a swap, not a rewrite.

**How does the parser work?** JSON lines: `json.loads` then a schema check (object, known keys, no nested values in string fields). Text: one anchored regex
`<date> <time> <SEVERITY> [service] rest`, then trailing `latency_ms=`/`event_id=` tokens are peeled off the message. Every failure becomes a `ParsedRecord` with an error code – parsers never raise and never lose the raw line.

**How are duplicates detected?** Three layers: (1) within the file/batch (first occurrence wins), (2) against the database (batched `IN (…)` lookup of `event_id` + `payload_hash`),
(3) the primary key + `INSERT OR IGNORE` as the final safety net (also covers concurrent writers). Same ID + same payload = duplicate (counted). Same ID + different payload = conflict (quarantined, original kept).

**Why deterministic IDs for text?** Text lines have no ID. Hashing the *normalised* content makes the same event produce the same ID on every run, from any file, regardless of severity alias or whitespace.

**How are invalid records quarantined?** Parse/validation failures are turned into `rejected_logs` rows with all reason codes, stage, provenance and the verbatim raw line. The raw line is part of a stable `reject_key`, so reruns don’t duplicate quarantine rows.

**Why is each file one transaction?** Atomicity at a natural boundary: either a file is fully loaded or not at all, so partial failure is safe and “re-run” is the recovery procedure.

**Why are late events kept?** They are valid data. Dropping them would silently under-count history. They’re flagged (`is_late`) and analytics use event time, so they are attributed correctly. Out-of-order events need no special handling because nothing depends on arrival order (output is sorted by event time).

**Why pandas only in analytics?** Row-level parsing/validation in plain Python is simple and keeps per-record error handling explicit; set-based aggregation (groupby, quantiles) is where pandas is the right tool. The dashboard reuses the same functions, so numbers cannot disagree.

**How was output verified?** (1) invariants (`input = inserted + duplicates + rejected`, CSV vs table counts, pandas vs SQL aggregates); (2) independent ground truth from the generator (its own labels, own percentile implementation); (3) rerun consistency; (4) 150 unit/integration tests.

**Performance findings.** The first profile contradicted the textbook assumption: DB writes were ~20 % of the time, normalisation ~50 %. `executemany` alone changed nothing measurable; targeted fast paths gave ≈ 12 %. Results are in README → Performance.

**Trade-offs worth knowing.** Default behaviour re-reads known files (so reruns report `Duplicates N`), `--skip-processed` avoids that. Fractional seconds are truncated. `DEBUG` is invalid by spec. Day/month-first dates are rejected rather than guessed.
