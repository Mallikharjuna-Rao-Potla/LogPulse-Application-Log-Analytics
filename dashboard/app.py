"""
LogPulse Dashboard
Run from the project root:
    python -m streamlit run dashboard/app.py

Reads the SQLite warehouse produced by run_pipeline.py.
Uses src.analytics to maintain consistency with analytics_report.json.
"""

import sqlite3
import sys
from pathlib import Path

import pandas as pd
import streamlit as st


# ---------------------------------------------------------
# Project configuration
# ---------------------------------------------------------

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT))

from src import analytics, config  # noqa: E402


# ---------------------------------------------------------
# Page configuration
# ---------------------------------------------------------

st.set_page_config(
    page_title="LogPulse",
    page_icon="📈",
    layout="wide"
)

st.title("LogPulse - Application Log Analytics")


# ---------------------------------------------------------
# Database configuration
# ---------------------------------------------------------

# Always use the verified 100K database by default.
DEFAULT_DB_PATH = PROJECT_ROOT / "data" / "logpulse_demo.db"

db_path = st.sidebar.text_input(
    "Database",
    value=str(DEFAULT_DB_PATH),
    key="logpulse_100k_database"
)

db_file = Path(db_path).expanduser()

if not db_file.is_absolute():
    db_file = PROJECT_ROOT / db_file

db_file = db_file.resolve()

if not db_file.exists():
    st.error(f"Database not found: {db_file}")
    st.code(
        "python run_pipeline.py --dataset 100k "
        "--db data/logpulse_demo.db "
        "--output-dir data/outputs_100k --reset-db"
    )
    st.stop()


# ---------------------------------------------------------
# Load database
# ---------------------------------------------------------

@st.cache_data(ttl=15, show_spinner=False)
def load(path: str):

    conn = sqlite3.connect(path)

    try:
        logs = analytics.load_processed_df(conn)

        rejected = pd.read_sql_query(
            """
            SELECT
                reject_id,
                run_id,
                stage,
                primary_reason,
                rejection_reasons,
                source_file,
                source_line,
                raw_record
            FROM rejected_logs
            ORDER BY reject_id
            """,
            conn
        )

        runs = pd.read_sql_query(
            """
            SELECT *
            FROM pipeline_runs
            ORDER BY started_at DESC
            """,
            conn
        )

    finally:
        conn.close()

    return logs, rejected, runs


logs, rejected, runs = load(str(db_file))


# ---------------------------------------------------------
# Database diagnostics
# ---------------------------------------------------------

st.sidebar.markdown("---")
st.sidebar.subheader("Database Information")

st.sidebar.write("Database:", db_file.name)
st.sidebar.write("Records loaded:", f"{len(logs):,}")

if st.sidebar.button("Reload data"):
    st.cache_data.clear()
    st.rerun()


if logs.empty:
    st.warning("No processed logs found in the selected database.")
    st.stop()


# ---------------------------------------------------------
# Prepare timestamps
# ---------------------------------------------------------

logs["ts"] = pd.to_datetime(
    logs["event_timestamp"],
    utc=True,
    errors="coerce"
)

logs = logs.dropna(subset=["ts"])

if logs.empty:
    st.warning("No valid timestamps found.")
    st.stop()


# ---------------------------------------------------------
# Sidebar filters
# ---------------------------------------------------------

st.sidebar.header("Filters")

dmin = logs["ts"].dt.date.min()
dmax = logs["ts"].dt.date.max()

picked = st.sidebar.date_input(
    "Date range",
    value=(dmin, dmax),
    min_value=dmin,
    max_value=dmax
)

if isinstance(picked, (tuple, list)) and len(picked) == 2:
    start, end = picked
elif isinstance(picked, (tuple, list)) and len(picked) == 1:
    start = end = picked[0]
else:
    start = end = picked


available_severities = sorted(
    logs["severity"].dropna().unique().tolist()
)

severities = st.sidebar.multiselect(
    "Severity",
    available_severities,
    default=available_severities
)

available_services = sorted(
    logs["service_name"].dropna().unique().tolist()
)

services = st.sidebar.multiselect(
    "Service",
    available_services,
    default=available_services
)


# ---------------------------------------------------------
# Apply filters
# ---------------------------------------------------------

mask = (
    (logs["ts"].dt.date >= start)
    & (logs["ts"].dt.date <= end)
    & logs["severity"].isin(severities)
    & logs["service_name"].isin(services)
)

df = logs.loc[mask].copy()

if df.empty:
    st.warning("No records match the current filters.")
    st.stop()


# ---------------------------------------------------------
# Analytics calculations
# ---------------------------------------------------------

ea = analytics.error_analytics(df)
la = analytics.latency_analytics(df)


# ---------------------------------------------------------
# Key performance indicators
# ---------------------------------------------------------

st.subheader("Key Performance Indicators")

c = st.columns(5)

c[0].metric(
    "Processed logs",
    f"{ea['total_logs']:,}"
)

c[1].metric(
    "Errors (ERROR + CRITICAL)",
    f"{ea['error_count']:,}"
)

c[2].metric(
    "Error rate",
    f"{ea['error_rate_pct']:.2f}%"
)

c[3].metric(
    "Avg latency",
    f"{la['avg_ms']:.0f} ms"
    if la["avg_ms"] is not None else "N/A"
)

c[4].metric(
    "P95 latency",
    f"{la['p95_ms']:.0f} ms"
    if la["p95_ms"] is not None else "N/A"
)


st.divider()


# ---------------------------------------------------------
# Dashboard tabs
# ---------------------------------------------------------

tab_over, tab_err, tab_lat, tab_rej, tab_runs = st.tabs(
    [
        "Overview",
        "Errors by Service",
        "Latency",
        "Rejected Records",
        "Pipeline Runs"
    ]
)


# ---------------------------------------------------------
# Overview
# ---------------------------------------------------------

with tab_over:

    left, right = st.columns(2)

    left.subheader("Severity Distribution")

    left.bar_chart(
        pd.Series(
            ea["severity_counts"],
            name="Logs"
        )
    )

    grain = (
        "daily"
        if len(ea["error_trend"]["daily"]) >= 3
        else "hourly"
    )

    right.subheader(f"Error Trend ({grain})")

    trend = pd.DataFrame(
        ea["error_trend"][grain]
    ).set_index("period")

    right.line_chart(trend[["errors"]])

    st.subheader("Error Rate Trend")

    st.line_chart(trend[["error_rate_pct"]])


# ---------------------------------------------------------
# Errors by service
# ---------------------------------------------------------

with tab_err:

    st.subheader("Service-wise Error Comparison")

    by = pd.DataFrame(
        ea["error_rate_by_service"]
    ).T

    if by.empty:
        st.info("No service-level error data available.")
    else:
        st.bar_chart(by["error_rate_pct"])

        st.dataframe(
            by,
            use_container_width=True
        )


# ---------------------------------------------------------
# Latency analysis
# ---------------------------------------------------------

with tab_lat:

    st.subheader("Latency Comparison by Service (ms)")

    by = pd.DataFrame(
        la["by_service"]
    ).T

    if by.empty:
        st.info("No service-level latency data available.")
    else:
        st.bar_chart(
            by[["avg_ms", "p50_ms", "p95_ms"]]
        )

        st.dataframe(
            by,
            use_container_width=True
        )

    st.subheader("Latency Trend")

    latency_grain = (
        "daily"
        if len(la["trend"]["daily"]) >= 3
        else "hourly"
    )

    lt = pd.DataFrame(
        la["trend"][latency_grain]
    ).set_index("period")

    if not lt.empty:
        st.line_chart(
            lt[["avg_ms", "p95_ms"]]
        )


# ---------------------------------------------------------
# Rejected records
# ---------------------------------------------------------

with tab_rej:

    st.subheader(
        f"Rejected / Quarantined Records ({len(rejected):,})"
    )

    if rejected.empty:
        st.success("Nothing rejected.")

    else:
        reasons = rejected["primary_reason"].value_counts()

        st.bar_chart(reasons)

        selected_reasons = st.multiselect(
            "Filter by rejection reason",
            list(reasons.index),
            default=list(reasons.index)
        )

        filtered_rejected = rejected[
            rejected["primary_reason"].isin(selected_reasons)
        ]

        st.dataframe(
            filtered_rejected,
            use_container_width=True,
            height=400
        )


# ---------------------------------------------------------
# Pipeline execution history
# ---------------------------------------------------------

with tab_runs:

    st.subheader("Pipeline Execution History")

    if runs.empty:
        st.info("No pipeline execution history available.")
    else:
        st.dataframe(
            runs,
            use_container_width=True
        )


# ---------------------------------------------------------
# Footer
# ---------------------------------------------------------

st.divider()

st.caption(
    "LogPulse | Application Log Analytics | "
    "Powered by Python, SQLite, Pandas and Streamlit"
)