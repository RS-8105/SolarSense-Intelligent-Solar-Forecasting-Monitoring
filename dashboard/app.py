
import json
import time
from pathlib import Path

import pandas as pd
import streamlit as st

# ============================================================
# CONFIGURATION
# ============================================================

PROJECT_ROOT = Path(__file__).resolve().parent.parent
DATA_DIR = PROJECT_ROOT / "data" / "processed" / "solar_readings_live"
REFRESH_SECONDS = 10

st.set_page_config(
    page_title="Smart Solar Analytics",
    page_icon="☀️",
    layout="wide",
)

# ============================================================
# STYLING
# ============================================================

st.markdown(
    """
    <style>
    .block-container {
        padding-top: 1.5rem;
        padding-bottom: 2rem;
    }

    div[data-testid="stMetric"] {
        background-color: rgba(128, 128, 128, 0.08);
        padding: 16px;
        border-radius: 12px;
        border: 1px solid rgba(128, 128, 128, 0.18);
    }
    </style>
    """,
    unsafe_allow_html=True,
)

# ============================================================
# DATA FILE SIGNATURE
# Detect newly created or modified Spark output files.
# ============================================================

def get_file_signature():
    if not DATA_DIR.exists():
        return ()

    signature = []

    for file in DATA_DIR.glob("*.json"):
        try:
            stat = file.stat()
            signature.append(
                (file.name, stat.st_mtime_ns, stat.st_size)
            )
        except OSError:
            continue

    return tuple(sorted(signature))


# ============================================================
# LOAD SPARK OUTPUT
# ============================================================

@st.cache_data(show_spinner=False, max_entries=3)
def load_data(file_signature):
    # The signature is used as part of the cache key.
    del file_signature

    if not DATA_DIR.exists():
        return pd.DataFrame()

    records = []

    for file in DATA_DIR.glob("*.json"):
        try:
            with file.open("r", encoding="utf-8") as handle:
                for line in handle:
                    line = line.strip()

                    if not line:
                        continue

                    try:
                        record = json.loads(line)

                        if isinstance(record, dict):
                            records.append(record)

                    except json.JSONDecodeError:
                        continue

        except OSError:
            continue

    if not records:
        return pd.DataFrame()

    df = pd.DataFrame(records)

    if "DATE_TIME" not in df.columns:
        return pd.DataFrame()

    df["DATE_TIME"] = pd.to_datetime(
        df["DATE_TIME"],
        errors="coerce",
    )

    if "INGESTION_TIME" in df.columns:
        df["INGESTION_TIME"] = pd.to_datetime(
            df["INGESTION_TIME"],
            errors="coerce",
        )

    df = df.dropna(subset=["DATE_TIME"])

    numeric_columns = [
        "AC_POWER",
        "DC_POWER",
        "DAILY_YIELD",
        "TOTAL_YIELD",
        "IRRADIATION",
        "AMBIENT_TEMPERATURE",
        "MODULE_TEMPERATURE",
        "EXPECTED_MIN_AC_POWER",
    ]

    for column in numeric_columns:
        if column in df.columns:
            df[column] = pd.to_numeric(
                df[column],
                errors="coerce",
            )

    boolean_columns = [
        "DATA_QUALITY_ISSUE",
        "ZERO_GENERATION_ANOMALY",
        "LOW_GENERATION_ANOMALY",
    ]

    for column in boolean_columns:
        if column in df.columns:
            df[column] = (
                df[column]
                .astype(str)
                .str.strip()
                .str.lower()
                .eq("true")
            )
        else:
            df[column] = False

    # Remove duplicate copies of identical measurements.
    identity_columns = [
        column
        for column in [
            "DATE_TIME",
            "AC_POWER",
            "DC_POWER",
            "IRRADIATION",
            "AMBIENT_TEMPERATURE",
            "MODULE_TEMPERATURE",
        ]
        if column in df.columns
    ]

    if identity_columns:
        # Keep the copy with the latest ingestion timestamp.
        if (
            "INGESTION_TIME" in df.columns
            and df["INGESTION_TIME"].notna().any()
        ):
            df = df.sort_values("INGESTION_TIME")

        df = df.drop_duplicates(
            subset=identity_columns,
            keep="last",
        )

    sort_column = (
        "INGESTION_TIME"
        if "INGESTION_TIME" in df.columns
        and df["INGESTION_TIME"].notna().any()
        else "DATE_TIME"
    )

    return (
        df.sort_values(sort_column)
        .reset_index(drop=True)
    )


# ============================================================
# SIDEBAR
# ============================================================

st.sidebar.title("☀️ Smart Solar")
st.sidebar.caption(
    "Generation monitoring and anomaly analysis"
)
st.sidebar.divider()

if st.sidebar.button(
    "Refresh data now",
    use_container_width=True,
):
    st.cache_data.clear()
    st.rerun()

st.sidebar.caption("Source: Spark processed JSON files")
st.sidebar.caption(
    f"Automatic refresh: every {REFRESH_SECONDS} seconds"
)

record_view = st.sidebar.radio(
    "Choose records",
    [
        "Historical dataset",
        "Test and recent readings",
        "All readings",
    ],
    index=0,
)

# ============================================================
# DASHBOARD CONTENT
# The fragment refreshes the analytics section automatically.
# ============================================================

@st.fragment(run_every=REFRESH_SECONDS)
def render_dashboard(record_view):
    signature = get_file_signature()
    df = load_data(signature)

    st.title("Smart Solar Analytics Dashboard")

    st.write(
        "Monitor solar generation, environmental conditions, "
        "and potential low-generation anomalies."
    )

    st.caption(
        f"Last dashboard update: {time.strftime('%H:%M:%S')}"
    )

    if df.empty:
        st.warning(
            "No processed readings found. Check whether Spark "
            "is writing JSON files to "
            "data/processed/solar_readings_live/."
        )
        return

    # --------------------------------------------------------
    # SELECT DATASET
    # --------------------------------------------------------

    if record_view == "Historical dataset":
        selected = df[
            df["DATE_TIME"].dt.year == 2020
        ].copy()

    elif record_view == "Test and recent readings":
        # Use Spark ingestion time rather than the historical
        # solar event time to identify recently processed data.
        if (
            "INGESTION_TIME" in df.columns
            and df["INGESTION_TIME"].notna().any()
        ):
            latest_ingestion = df["INGESTION_TIME"].max()
            cutoff = latest_ingestion - pd.Timedelta(minutes=5)

            selected = df[
                df["INGESTION_TIME"] >= cutoff
            ].copy()
        else:
            selected = df.iloc[0:0].copy()

    else:
        selected = df.copy()

    sort_column = (
        "INGESTION_TIME"
        if "INGESTION_TIME" in selected.columns
        and selected["INGESTION_TIME"].notna().any()
        else "DATE_TIME"
    )

    selected = selected.sort_values(sort_column)

    if selected.empty:
        st.warning(
            f"No readings available for '{record_view}'. "
            "Try selecting All readings. For recent readings, "
            "check that the producer and Spark are running."
        )
        return

    # --------------------------------------------------------
    # SYSTEM OVERVIEW
    # --------------------------------------------------------

    latest = selected.iloc[-1]

    st.subheader("System Overview")

    col1, col2, col3, col4 = st.columns(4)

    col1.metric(
        "Latest AC Power",
        f"{latest.get('AC_POWER', 0):,.2f}",
    )

    col2.metric(
        "Latest Irradiation",
        f"{latest.get('IRRADIATION', 0):.3f}",
    )

    col3.metric(
        "Low-Generation Flags",
        int(selected["LOW_GENERATION_ANOMALY"].sum()),
    )

    col4.metric(
        "Data Quality Issues",
        int(selected["DATA_QUALITY_ISSUE"].sum()),
    )

    st.caption(
        f"Distinct readings: {len(selected):,} · "
        f"Zero-generation flags: "
        f"{int(selected['ZERO_GENERATION_ANOMALY'].sum()):,} · "
        f"Solar event time: {latest['DATE_TIME']} · "
        f"Spark ingestion time: "
        f"{latest.get('INGESTION_TIME', 'Unavailable')}"
    )

    st.divider()

    # --------------------------------------------------------
    # GENERATION SUMMARY
    # --------------------------------------------------------

    st.subheader("Generation Summary")

    col1, col2, col3 = st.columns(3)

    ac_power = selected["AC_POWER"].dropna()

    if not ac_power.empty:
        col1.metric(
            "Average AC Power",
            f"{ac_power.mean():,.2f}",
        )
        col2.metric(
            "Maximum AC Power",
            f"{ac_power.max():,.2f}",
        )
        col3.metric(
            "Minimum AC Power",
            f"{ac_power.min():,.2f}",
        )
    else:
        col1.metric("Average AC Power", "N/A")
        col2.metric("Maximum AC Power", "N/A")
        col3.metric("Minimum AC Power", "N/A")

    st.caption(
        "Power statistics use the dataset's original units; "
        "they are not energy totals."
    )

    st.divider()

    # --------------------------------------------------------
    # ENVIRONMENTAL CONDITIONS
    # --------------------------------------------------------

    st.subheader("Environmental Conditions")

    left, right = st.columns(2)

    with left:
        st.markdown("**Irradiation over time**")

        irradiation = selected.set_index("DATE_TIME")[
            ["IRRADIATION"]
        ].dropna()

        st.line_chart(irradiation, height=300)

    with right:
        st.markdown("**Temperature over time**")

        temperature_columns = [
            column
            for column in [
                "AMBIENT_TEMPERATURE",
                "MODULE_TEMPERATURE",
            ]
            if column in selected.columns
        ]

        if temperature_columns:
            temperatures = selected.set_index("DATE_TIME")[
                temperature_columns
            ].dropna(how="all")

            st.line_chart(temperatures, height=300)

    st.divider()

    # --------------------------------------------------------
    # POWER GENERATION TREND
    # --------------------------------------------------------

    st.subheader("Solar Power Generation")

    power_columns = [
        column
        for column in ["AC_POWER", "DC_POWER"]
        if column in selected.columns
    ]

    if power_columns:
        power_data = selected.set_index("DATE_TIME")[
            power_columns
        ].dropna(how="all")

        st.line_chart(power_data, height=350)

    st.divider()

    # --------------------------------------------------------
    # ANOMALY ANALYSIS
    # --------------------------------------------------------

    st.subheader("Anomaly Analysis")

    anomaly_filter = st.selectbox(
        "Filter readings",
        [
            "All readings",
            "Low-generation anomalies",
            "Zero-generation anomalies",
            "Data quality issues",
        ],
        key=f"anomaly_filter_{record_view}",
    )

    if anomaly_filter == "Low-generation anomalies":
        table_data = selected[
            selected["LOW_GENERATION_ANOMALY"]
        ]

    elif anomaly_filter == "Zero-generation anomalies":
        table_data = selected[
            selected["ZERO_GENERATION_ANOMALY"]
        ]

    elif anomaly_filter == "Data quality issues":
        table_data = selected[
            selected["DATA_QUALITY_ISSUE"]
        ]

    else:
        table_data = selected

    st.caption(
        f"Showing {len(table_data):,} matching readings"
    )

    preferred_columns = [
        "DATE_TIME",
        "INGESTION_TIME",
        "AC_POWER",
        "DC_POWER",
        "IRRADIATION",
        "AMBIENT_TEMPERATURE",
        "MODULE_TEMPERATURE",
        "EXPECTED_MIN_AC_POWER",
        "LOW_GENERATION_ANOMALY",
        "ZERO_GENERATION_ANOMALY",
        "DATA_QUALITY_ISSUE",
    ]

    display_columns = [
        column
        for column in preferred_columns
        if column in table_data.columns
    ]

    st.dataframe(
        table_data[display_columns].tail(100).iloc[::-1],
        use_container_width=True,
        hide_index=True,
    )

    # --------------------------------------------------------
    # CSV EXPORT
    # --------------------------------------------------------

    st.download_button(
        "Download filtered readings as CSV",
        data=selected.to_csv(index=False).encode("utf-8"),
        file_name="solar_readings.csv",
        mime="text/csv",
    )


render_dashboard(record_view)
