# LogPulse – Application Log Processing & Analytics

A Python-based data engineering pipeline for processing structured and semi-structured application logs, validating records, handling duplicates, and generating operational analytics.

**Live Dashboard:** [LogPulse – Streamlit App](https://logpulse-application-log-analytics.streamlit.app/)

## 1. Overview

LogPulse converts raw application logs into structured, validated, and queryable data.

The pipeline supports:
- JSON Lines (`.jsonl`) and text (`.log`, `.txt`) files.
- Timestamp and severity normalization.
- Field extraction and data validation.
- Duplicate detection and invalid-record quarantine.
- SQLite storage and analytical reporting.
- Streamlit dashboard for data exploration.

## 2. Pipeline Architecture

```text
 JSON Lines (.jsonl)       Text Logs (.log / .txt)
          |                         |
          +-----------+-------------+
                      |
                  Ingestion
          (File discovery, checksum,
             source and line tracking)
                      |
                    Parsing
             (JSON parser / Regex)
                      |
                Normalization
       (UTC timestamps, severity aliases,
          numeric values, generated IDs)
                      |
                  Validation
       (Required fields, formats, ranges,
             known-service checks)
                      |
             +--------+--------+
             |                 |
        Valid Records     Invalid Records
             |                 |
        Deduplication       Quarantine
             |          (Raw line + reason codes)
             |
        SQLite Storage
             |
       Analytics & Reports
             |
       CSV / JSON / Charts
             |
        Streamlit Dashboard
```

## 3. Live Dashboard

**Application:** [Open LogPulse Dashboard](https://logpulse-application-log-analytics.streamlit.app/)

The dashboard provides:
- Total processed records and error rate.
- Error distribution by service and severity.
- Average and percentile latency.
- Rejected-record analysis.
- Pipeline execution history.

## 4. Technology Stack

| Component | Technology |
|---|---|
| Programming | Python |
| Data Processing | Pandas, NumPy |
| Parsing | JSON, Regex |
| Database | SQLite |
| Analytics | SQL, Pandas |
| Visualization | Streamlit |
| Testing | Pytest |
| Version Control | Git |

## 5. Installation

### Prerequisites

- Python 3.10 or later
- pip
- Git

### Windows

```powershell
cd path\to\LogPulse

python -m venv .venv

.venv\Scripts\Activate.ps1

python -m pip install --upgrade pip

python -m pip install -r requirements.txt
```

If PowerShell blocks virtual environment activation:

```powershell
.venv\Scripts\python.exe -m pip install -r requirements.txt
```

### Linux / macOS

```bash
cd path/to/LogPulse

python3 -m venv .venv

source .venv/bin/activate

python -m pip install --upgrade pip

python -m pip install -r requirements.txt
```

## 6. Running the Pipeline

Run the default sample dataset:

```bash
python run_pipeline.py
```

Run the 10K dataset:

```bash
python run_pipeline.py --dataset 10k --reset-db
```

Run the 100K dataset:

```bash
python run_pipeline.py --dataset 100k --reset-db
```

Process custom log files:

```bash
python run_pipeline.py --input path/to/logs.jsonl path/to/application.log --db data/custom.db
```

Run the Streamlit dashboard:

```bash
python -m streamlit run dashboard/app.py
```

Open `http://localhost:8501` in your browser.

## 7. Data Quality & Processing Results

The pipeline classifies input records into inserted, duplicate, and rejected categories.

| Dataset | Input Records | Inserted | Duplicates | Rejected |
|---|---:|---:|---:|---:|
| 100 | 100 | 84 | 2 | 14 |
| 10K | 10,000 | 8,771 | 200 | 1,029 |
| 100K | 100,000 | 87,710 | 2,000 | 10,290 |

**Data reconciliation:**

`Input Records = Inserted + Duplicates + Rejected`

Invalid records are retained in quarantine with their original log content and validation reason codes for traceability.

## 8. Database & Reliability

SQLite is used for persistent storage.

| Table | Purpose |
|---|---|
| `processed_logs` | Stores validated log records |
| `rejected_logs` | Stores invalid records and rejection reasons |
| `pipeline_runs` | Tracks pipeline execution details |
| `processed_files` | Maintains file-processing information |
| `services` | Stores service reference information |

Reliability mechanisms include:
- Deterministic record identifiers.
- Duplicate detection and conflict handling.
- SHA-256 file checksums.
- Tracking of previously processed files.
- Transaction-based database operations.
- Rollback and retry handling.
- Late and out-of-order event identification.

## 9. Analytics & Outputs

The pipeline generates operational metrics, including:

- **Error Rate:** Percentage of error-level records among processed records.
- **Service Analysis:** Error distribution across application services.
- **Latency Analysis:** Minimum, maximum, average, P50, and P95 latency.
- **Time-Based Trends:** Hourly and daily log activity.

Generated reports include CSV and JSON outputs containing processing summaries and analytical results.

## 10. Testing & Validation

Run the automated test suite:

```bash
python -m pytest -v
```

**Test results:** 150 tests passed.

Additional validation:
- 54/54 independent expected-result checks passed for the sample dataset.
- 55/55 independent expected-result checks passed for the larger datasets.
- Record reconciliation and database consistency checks.
- Duplicate handling and repeated-run validation.
- Malformed input and partial-failure scenarios.

## 11. Performance

Recorded benchmark results:

| Dataset | Records | Processing Time | Throughput |
|---|---:|---:|---:|
| Sample | 100 | 0.005 sec | — |
| 10K | 10,000 | 0.388 sec | ~25,800 records/sec |
| 100K | 100,000 | 3.909 sec | ~25,600 records/sec |

Performance measurements are environment-dependent and should be treated as recorded benchmark results rather than guaranteed execution times.

Run the benchmark:

```bash
python -m src.benchmark
```

## 12. Project Structure

```text
LogPulse/
│
├── dashboard/
│   └── app.py
│
├── data/
│   └── outputs/
│
├── src/
│   ├── analytics.py
│   ├── config.py
│   ├── generator.py
│   ├── ingestion.py
│   ├── parser.py
│   ├── pipeline.py
│   ├── storage.py
│   ├── transformation.py
│   └── validation.py
│
├── tests/
│
├── requirements.txt
├── run_pipeline.py
└── README.md
```

## 13. Scope & Limitations

- Uses SQLite for local persistent storage.
- Designed for batch log processing rather than continuous streaming.
- Processing large files may require additional memory.
- Distributed processing and real-time message streaming are outside the current implementation scope.

## 14. Additional Commands

```bash
# Generate sample data
python -m src.generator

# Run performance benchmark
python -m src.benchmark

# Display available pipeline options
python run_pipeline.py --help
