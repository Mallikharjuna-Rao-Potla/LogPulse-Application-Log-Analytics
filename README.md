# LogPulse --- Application Log Processing & Analytics Pipeline

An end-to-end data engineering project for ingesting, parsing,
validating, normalising, storing, and analysing structured and
semi-structured application logs.

LogPulse processes JSON Lines (`.jsonl`) and plain-text (`.log`, `.txt`)
files, preserves invalid records in a quarantine table, prevents
unintended duplicates, supports safe reruns and recovery, and produces
analytics and reconciliation reports. A Streamlit dashboard is included
for interactive inspection.

The project uses a fixed random seed (`42`) so generated datasets and
expected results are reproducible.

------------------------------------------------------------------------

## 1. Project objectives

The goal is to turn inconsistent application logs into reliable,
queryable data that operations teams can use to understand service
health.

LogPulse is designed to:

-   Ingest structured JSON Lines and semi-structured text logs.
-   Parse records and extract common fields.
-   Normalise timestamps, severity values, identifiers, and latency.
-   Validate records against required-field, format, range, and
    reference-data rules.
-   Preserve rejected records with their original input and clear reason
    codes.
-   Store valid records in SQLite with primary-key, foreign-key, and
    other constraints.
-   Detect duplicates and conflicting event IDs.
-   Support reruns, retries, and backfills without unintended duplicate
    inserts.
-   Calculate error-rate, service-level, time-trend, and latency
    metrics.
-   Reconcile pipeline output against independent expected results.
-   Provide automated tests and an optional dashboard.

## 2. Quick start

### Requirements

-   Python 3.10 or later (developed and tested with Python 3.12).
-   pip.
-   Windows, macOS, or Linux.

SQLite is included with Python. No separate database server,
credentials, or external dataset download is required for the bundled
sample.

### Setup

**Windows PowerShell**

``` powershell
cd path\to\LogPulse
python -m venv .venv
.venv\Scripts\Activate.ps1
python -m pip install --upgrade pip
python -m pip install -r requirements.txt
```

If PowerShell blocks activation, use:

``` powershell
.venv\Scripts\python.exe -m pip install -r requirements.txt
```

**macOS / Linux**

``` bash
cd path/to/LogPulse
python3 -m venv .venv
source .venv/bin/activate
python -m pip install --upgrade pip
python -m pip install -r requirements.txt
```

### Run the sample pipeline

``` bash
python run_pipeline.py
```

This processes the bundled 100-record sample. The command prints
ingestion counts, analytics, and reconciliation results. Generated
reports are written under `data/outputs/`.

## 3. Common commands

Run commands from the project root.

  ----------------------------------------------------------------------------------------
  Task                                Command
  ----------------------------------- ----------------------------------------------------
  Process the bundled 100-record      `python run_pipeline.py`
  sample                              

  Process the 10,000-record dataset   `python run_pipeline.py --dataset 10k --reset-db`

  Process the 100,000-record dataset  `python run_pipeline.py --dataset 100k --reset-db`

  Run tests                           `python -m pytest -v`

  Run benchmark                       `python -m src.benchmark`

  Regenerate deterministic datasets   `python -m src.generator`

  Start dashboard                     `python -m streamlit run dashboard/app.py`

  View CLI options                    `python run_pipeline.py --help`
  ----------------------------------------------------------------------------------------

To process your own log files:

``` bash
python run_pipeline.py --input path/to/logs.jsonl path/to/application.log --db data/custom.db
```

Use the actual paths to your input files. The input option accepts one
or more files.

**Important:** `--reset-db` clears the selected database before
processing. Use it when you want a clean run or are switching between
datasets. Do not use it if you need to preserve existing database
records.

### Exit codes

    Exit code Meaning
  ----------- -----------------
          `0` Success
          `1` Partial failure
          `2` Failed

## 4. Architecture and processing flow

``` text
 JSON Lines (.jsonl)       Text logs (.log / .txt)
          |                         |
          +-----------+-------------+
                      |
                  Ingestion
          (file discovery, checksum,
             source and line tracking)
                      |
                    Parsing
             (JSON parser / regex)
                      |
                Normalisation
       (UTC timestamps, severity aliases,
          numeric values, generated IDs)
                      |
                  Validation
       (required fields, formats, ranges,
             known-service checks)
                      |
             +--------+--------+
             |                 |
        Valid records      Invalid records
             |                 |
        Deduplication       Quarantine
             |          (raw line + reason codes)
             |
        SQLite storage
             |
       Analytics and reports
             |
      CSV / JSON / charts
             |
      Optional Streamlit UI
```

Each input file is processed in its own database transaction. Run and
file-processing metadata are recorded to support traceability,
idempotency, and recovery.

For more detail, see [`docs/architecture.md`](docs/architecture.md).

## 5. Project structure

``` text
LogPulse/
├── run_pipeline.py             # Command-line entry point
├── requirements.txt            # Python dependencies
├── src/
│   ├── config.py               # Configuration and processing rules
│   ├── logger.py               # Logging setup
│   ├── ingestion.py            # File discovery and ingestion
│   ├── parser.py               # JSON and text parsing
│   ├── transformation.py       # Normalisation and derived fields
│   ├── validation.py           # Data-quality validation
│   ├── storage.py              # SQLite operations
│   ├── analytics.py            # Error and latency analytics
│   ├── reporting.py            # Report generation
│   ├── pipeline.py             # End-to-end orchestration
│   ├── generator.py            # Reproducible test-data generation
│   └── benchmark.py            # Performance measurement
├── database/
│   └── schema.sql              # Database schema and constraints
├── data/
│   ├── sample/                 # Bundled 100-record sample and ground truth
│   ├── generated/              # Generated 10k and 100k datasets
│   └── outputs/                # Pipeline reports and exports
├── dashboard/
│   └── app.py                  # Optional Streamlit dashboard
├── tests/                      # Automated tests
└── docs/
    ├── architecture.md
    ├── data_dictionary.md
    ├── design_decisions.md
    ├── ai_prompt_history.md
    ├── sample_run_output.txt
    └── test_evidence.txt
```

The exact generated output files depend on the dataset and command-line
options used.

## 6. Input formats and data generation

LogPulse supports two input formats.

### Structured JSON Lines

Each line must contain one JSON object. For example:

``` json
{"event_id":"EVT-SAMPLE-000001","timestamp":"2026-10-01T00:00:19Z","severity":"INFO","service_name":"orders","message":"Order status updated","latency_ms":134}
```

This is JSON Lines (`.jsonl`), not a single JSON array.

### Semi-structured text

Example:

``` text
2026-10-01 00:07:04 ERROR [payment] Transaction processing failed latency_ms=3598
```

Text records do not contain an event ID. LogPulse creates a
deterministic ID using a hash of the normalised content.

### Generated datasets

The data generator uses a fixed seed (`42`) and creates a mixture of
valid records and deliberate data-quality issues.

  -----------------------------------------------------------------------------
  Dataset        Input    Valid /   Duplicates   Rejected       Late     Out of
               records   inserted                                         order
  --------- ---------- ---------- ------------ ---------- ---------- ----------
  Sample           100         84            2         14          2          6

  10k           10,000      8,771          200      1,029        196        629

  100k         100,000     87,710        2,000     10,290      1,960      6,384
  -----------------------------------------------------------------------------

These are the generator's expected counts. The pipeline's reconciliation
report checks its output against independently generated ground-truth
values.

Generated data includes:

-   Four severity levels and seven known services.
-   Duplicate records and duplicate event IDs.
-   Missing required fields.
-   Invalid timestamps and severity values.
-   Negative, non-numeric, and unusually high latency values.
-   Unknown services.
-   Malformed text lines and unexpected JSON structures.
-   Late-arriving and out-of-order events.
-   Variations such as `warn` and `err`, timezone offsets, string-form
    latency, and alternate field names such as `level` and `service`.

## 7. Normalisation and validation rules

### Normalisation

The pipeline standardises input values before storage:

-   Timestamps are converted to UTC.
-   Severity aliases are mapped to the supported severity values.
-   Numeric fields such as latency are converted to a consistent type.
-   Text events receive deterministic event IDs.
-   Common alternate field names are handled where supported.
-   Derived fields, including late-event and date/time fields, are
    prepared for analytics.

### Validation

Records are checked for:

-   Required fields.
-   Event ID format.
-   Valid timestamp and permitted timestamp range.
-   Supported severity.
-   Present and known service name.
-   Non-empty message.
-   Numeric, non-negative latency not exceeding 10 minutes.
-   Duplicate event IDs and conflicting duplicate payloads.
-   Unexpected JSON structure.

`latency_ms` is optional. Other required fields must be present and
valid.

Invalid records are not silently discarded. They are written to
`rejected_logs` with the processing stage, reason code(s), source
filename, line number, and original raw line. A record can have more
than one validation reason.

## 8. Database design

The project uses SQLite. The database contains these tables:

  -----------------------------------------------------------------------
  Table                               Purpose
  ----------------------------------- -----------------------------------
  `processed_logs`                    Stores valid, normalised log
                                      records. `event_id` is the primary
                                      key.

  `rejected_logs`                     Stores rejected records and their
                                      traceable reason codes.

  `pipeline_runs`                     Stores information about pipeline
                                      executions.

  `processed_files`                   Tracks source-file checksums and
                                      processing history.

  `services`                          Reference table for known services.
  -----------------------------------------------------------------------

Database safeguards include:

-   Primary-key and foreign-key constraints.
-   `CHECK` constraints for severity, latency, and supported flags.
-   Foreign-key enforcement using `PRAGMA foreign_keys=ON`.
-   A unique rejection key to prevent repeated quarantine entries.
-   Indexes for common timestamp, service, severity, date, and run
    queries.

See [`database/schema.sql`](database/schema.sql) for the DDL and
[`docs/data_dictionary.md`](docs/data_dictionary.md) for column
definitions.

## 9. Idempotency, retries, and backfills

The pipeline is designed to be safe to run more than once.

-   **Deterministic event IDs:** JSON records use their supplied event
    ID. Text records receive an ID derived from normalised content.
-   **Database uniqueness:** `event_id` is the primary key. Repeated
    records with the same ID and payload are counted as duplicates and
    are not inserted again.
-   **Conflicting IDs:** If the same event ID arrives with different
    content, the later record is quarantined with
    `DUPLICATE_EVENT_ID_CONFLICT`. The first stored record is retained.
-   **File checksums:** SHA-256 checksums identify files, including
    renamed copies with identical content.
-   **Skip option:** `--skip-processed` skips files already registered
    as processed. Without this option, the pipeline reads the file and
    reconciles its records as duplicates.
-   **Quarantine idempotency:** A deterministic rejection key prevents
    the same rejected line from being added repeatedly.
-   **Per-file transactions:** Each file is processed atomically. If a
    file fails, its transaction is rolled back while other files can
    continue.
-   **Retry and recovery:** Transient SQLite lock errors are retried. A
    failed file can be processed again by rerunning the pipeline.
-   **Backfill:** Older and newer files can be supplied in different
    orders. Events older than the configured warehouse watermark by more
    than 30 minutes are flagged as late but can still be stored.
-   **Event-time analytics:** Time-based analytics use the event
    timestamp rather than the order in which files were processed.

### Idempotency example

For the bundled sample, the expected behaviour is:

``` text
First run : Input 100 | Inserted 84 | Duplicates 2  | Rejected 14
Second run: Input 100 | Inserted 0  | Duplicates 86 | Rejected 14
```

The second run should not increase the number of valid or quarantined
records.

## 10. Analytics and reports

The main error-rate formula is:

``` text
Error Rate = (ERROR + CRITICAL) / Total Valid Logs * 100
```

The analytics layer reports:

-   Severity distribution.
-   Overall error rate.
-   Error rate by service.
-   Hourly and daily error trends.
-   Average, minimum, and maximum latency.
-   P50 and P95 latency.
-   Latency statistics by service and over time.
-   Operational metrics such as input count, inserted count, rejected
    count, duplicate count, processing duration, and throughput.

Records without a latency value are excluded from latency calculations.
Percentiles use linear interpolation.

### Output artefacts

The pipeline writes reports and exports under the configured output
directory. Typical outputs include:

-   `processed_logs.csv`
-   `rejected_logs.csv`
-   `analytics_report.json`
-   `reconciliation_report.json`
-   Charts.
-   Benchmark results, when the benchmark is run.

The exact output directory can be changed through the CLI options.

## 11. Reconciliation and correctness checks

LogPulse checks that pipeline results are internally consistent and
match independent expected results.

### Internal checks

The reconciliation process checks conditions such as:

-   Input records equal inserted records plus duplicates plus rejected
    records.
-   Database row counts match inserted counts.
-   Exported CSV row counts match the corresponding database tables.
-   Severity counts from pandas match SQL results.
-   Latency aggregates from pandas match SQL results.

### Ground-truth checks

The generator creates expected results independently of the pipeline.
Reconciliation compares counts, rejection reasons, late/out-of-order
counts, severity distribution, error rates, latency statistics, and
daily trends.

Documented baseline results:

  Dataset     Expected ground-truth checks
  --------- ------------------------------
  Sample                      54/54 passed
  10k                         55/55 passed
  100k                        55/55 passed

The pipeline also reports its internal checks. Refer to the run's
`reconciliation_report.json` for the actual result of that execution.

## 12. Performance

The original benchmark was measured in a single-process build
environment. Results are hardware- and environment-dependent.

  Dataset     Records   Final measured time        Throughput   Peak memory
  --------- --------- --------------------- ----------------- -------------
  Sample          100               0.005 s               ---         66 MB
  10k          10,000               0.388 s   25.8k records/s         86 MB
  100k        100,000               3.909 s   25.6k records/s        231 MB

The benchmark identified normalisation as a major part of processing
time. Fast paths for common timestamp and field formats, along with
caching repeated strings, reduced the measured runtime in that
benchmark. Batched writes did not provide a measurable improvement in
that SQLite test setup.

Run the benchmark on your own machine:

``` bash
python -m src.benchmark
```

Do not compare benchmark numbers across machines without noting the
environment, dataset size, and measurement method.

## 13. Configuration

Main configuration values are maintained in `src/config.py`. These
include severity aliases, known services, latency limits, late-event
thresholds, batch sizes, and retry settings.

Environment variables:

  Variable                   Purpose
  -------------------------- -----------------------------------
  `LOGPULSE_DB`              Database path
  `LOGPULSE_NO_FASTPATH=1`   Disables normalisation fast paths

Use the CLI help command to view supported arguments:

``` bash
python run_pipeline.py --help
```

## 14. Dashboard

The optional Streamlit dashboard provides a visual view of the processed
data, including:

-   Overview metrics.
-   Error counts and service-level error information.
-   Latency summaries.
-   Rejected records.
-   Pipeline run history.

Start it from the project root:

``` bash
python -m streamlit run dashboard/app.py
```

Streamlit will print a local URL, usually `http://localhost:8501`.

The dashboard reads from the selected SQLite database. Make sure it
points to the database created by the pipeline run you want to inspect.
If you switch datasets or database files, verify the selected path
before interpreting the metrics.

## 15. Testing

Run the automated test suite from the project root:

``` bash
python -m pytest -v
```

The project includes 150 pytest tests covering:

-   Parsing of JSON and text records.
-   File discovery and ingestion.
-   Timestamp, severity, and latency normalisation.
-   Required-field and reference-data validation.
-   SQLite schema, constraints, and storage.
-   Duplicate detection and idempotency.
-   Transactions, rollback, and retry behaviour.
-   Empty and missing inputs.
-   Malformed records and unexpected schemas.
-   Late and out-of-order events.
-   Analytics and report generation.
-   Generator reproducibility.
-   End-to-end reconciliation against expected results.

Test evidence is documented in
[`docs/test_evidence.txt`](docs/test_evidence.txt). Run the tests
locally rather than relying only on previously recorded results.

## 16. Assumptions

-   Timestamps without an explicit timezone are treated as UTC.
-   Fractional timestamp seconds are truncated to seconds.
-   Only supported ISO-8601-style timestamp formats are accepted.
    Ambiguous dates such as `05/11/2026` are rejected.
-   Supported severity values are `INFO`, `WARNING`, `ERROR`, and
    `CRITICAL`. Configured aliases such as `warn`, `err`, and `crit` are
    mapped.
-   `null`, empty strings, whitespace, `N/A`, and `-` are treated as
    missing values.
-   Latency is optional; other required fields are mandatory.
-   JSON input uses JSON Lines format, with one object per line.
-   The source filename stored in records is the basename. File identity
    is based on SHA-256 content, not the filename.

## 17. Known limitations

-   SQLite is a single-writer database. It is suitable for this project
    but is not intended for high-concurrency write workloads.
-   Each file is currently read into memory during processing. Chunked
    or streaming ingestion would be a next step for larger files.
-   Real-time streaming ingestion and automatic schema evolution are
    outside the current scope.
-   Dashboard verification should be performed in the target
    environment; a successful pipeline or test run does not by itself
    prove that the Streamlit interface works correctly.

## 18. Possible future improvements

-   Chunked and streaming file ingestion.
-   PostgreSQL or Parquet storage options.
-   Configurable alerts for error-rate and latency thresholds.
-   Data-quality scoring by source or service.
-   Scheduled execution using cron or Apache Airflow.
-   Docker packaging and CI automation.
-   Schema versioning and controlled schema evolution.

These are future extensions, not requirements for running the current
pipeline.

## 19. Design and implementation notes

For implementation choices and trade-offs, see:

-   [`docs/architecture.md`](docs/architecture.md)
-   [`docs/data_dictionary.md`](docs/data_dictionary.md)
-   [`docs/design_decisions.md`](docs/design_decisions.md)
-   [`docs/ai_prompt_history.md`](docs/ai_prompt_history.md)

The AI prompt history should contain only prompts that were actually
submitted to AI tools. Do not add reconstructed prompts as if they were
original conversation history.

------------------------------------------------------------------------

## Summary

LogPulse demonstrates an end-to-end batch data engineering workflow:
ingestion, parsing, normalisation, validation, quarantine, relational
storage, deduplication, idempotent reruns, recovery, analytics,
reconciliation, testing, and dashboard-based inspection.

The focus is not only on processing valid logs, but also on making
data-quality decisions traceable and demonstrating that the pipeline
produces consistent results.
