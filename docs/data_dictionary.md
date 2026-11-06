# Data dictionary

## Input fields
| Field | Required | Type | Notes |
|---|---|---|---|
| event_id | JSON: yes · text: generated | string `^[A-Za-z0-9][A-Za-z0-9_.:-]{0,63}$` | text logs get `GEN-<16 hex>` derived from normalised content |
| timestamp | yes | ISO-8601 `YYYY-MM-DD[T ]HH:MM:SS[.fff][Z|±HH:MM]` | no zone ⇒ UTC; stored as `YYYY-MM-DDTHH:MM:SSZ` |
| severity | yes | INFO / WARNING / ERROR / CRITICAL (aliases accepted) | aliases: info, warn, warning, err, error, crit, critical, information |
| service_name | yes | string, must exist in `services` | lower-cased, whitespace-trimmed |
| message | yes | string | whitespace collapsed |
| latency_ms | **optional** | number ≥ 0 and ≤ 600,000 | strings like `"120"`/`"120ms"` are converted |

Accepted alternative JSON key names: `id`, `time`/`ts`, `level`/`log_level`, `service`/`svc`, `msg`, `latency`.
Text format: `YYYY-MM-DD HH:MM:SS SEVERITY [service] message [event_id=…] [latency_ms=…]`.

## processed_logs
| Column | Type | Description |
|---|---|---|
| event_id | TEXT PK | idempotency key |
| event_timestamp | TEXT | UTC ISO-8601 |
| event_date / event_hour | TEXT | **derived** `YYYY-MM-DD` / `YYYY-MM-DDTHH:00:00Z` (trend buckets) |
| severity | TEXT | CHECK in the four levels |
| service_name | TEXT FK → services | |
| message | TEXT | |
| latency_ms | REAL NULL | CHECK ≥ 0 |
| is_error | INTEGER | **derived**: 1 for ERROR/CRITICAL |
| is_late | INTEGER | **derived**: older than watermark by > threshold when it arrived |
| source_format | TEXT | `json` / `text` |
| event_id_generated | INTEGER | 1 if the ID was derived |
| payload_hash | TEXT | hash of normalised content (conflict detection) |
| source_file, source_line, file_sha256 | | provenance |
| run_id FK → pipeline_runs, ingested_at | | lineage |

## rejected_logs
`reject_id` PK · `reject_key` UNIQUE (idempotent quarantine) · `run_id` · `stage` (PARSING/VALIDATION) · `primary_reason` ·
`rejection_reasons` (`;`-separated) · `event_id` (if known) · `source_format` · `source_file` · `source_line` · `file_sha256` · `raw_record` (verbatim) · `rejected_at`.

## pipeline_runs
`run_id` PK · timestamps · `status` (RUNNING/SUCCESS/PARTIAL_FAILURE/FAILED) · `failure_reason` · input_files · files_failed · input/inserted/duplicate/rejected/late/out_of_order records · processing_seconds · duration_seconds · throughput_rps.

## processed_files
`sha256` UNIQUE (file identity) · file_name · file_path · size_bytes · record_count · status (PROCESSED/FAILED) · last_error · times_seen · first/last run id · first/last seen.

## Rejection reason codes
| Code | Meaning |
|---|---|
| PARSE_ERROR_MALFORMED_JSON | line is not valid JSON (e.g. truncated) |
| PARSE_ERROR_UNEXPECTED_FORMAT | text line doesn’t match the log pattern |
| UNEXPECTED_SCHEMA | valid JSON but not a log event (array, scalar, no known keys, nested value in a string field) |
| MISSING_EVENT_ID / INVALID_EVENT_ID | absent/null/blank · bad characters or too long |
| MISSING_TIMESTAMP / INVALID_TIMESTAMP / TIMESTAMP_OUT_OF_RANGE | absent · unparseable (e.g. `05/11/2026`) · outside 2000–2100 |
| MISSING_SEVERITY / INVALID_SEVERITY | absent · not one of the four levels |
| MISSING_SERVICE_NAME / UNKNOWN_SERVICE | absent · not in the `services` reference table |
| MISSING_MESSAGE | absent/blank |
| INVALID_LATENCY_TYPE / NEGATIVE_LATENCY / LATENCY_EXCEEDS_MAX | non-numeric · < 0 · > 600,000 ms |
| DUPLICATE_EVENT_ID_CONFLICT | event_id already stored with different content |
Exact duplicates (same ID, same content) are **counted** as duplicates, not quarantined.
