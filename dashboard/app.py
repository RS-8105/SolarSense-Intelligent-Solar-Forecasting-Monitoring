
import json
import time
from pathlib import Path

import pandas as pd
import streamlit as st

# ============================================================
# CONFIGURATION
# ============================================================

PROJECT_ROOT = Path(__file__).resolve().parent.parent
DATA_DIR = PROJECT_ROOT / "data" / "processed" / "solar_readings"

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
# DATA LOADING
# ============================================================

@st.cache_data(ttl=REFRESH_SECONDS)
def load_data():
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
                        records.append(json.loads(line))
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

    # Collapse duplicate copies of identical measurements.
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

    df = df.drop_duplicates(
        subset=identity_columns,
        keep="last",
    )

    return (
        df.sort_values("DATE_TIME")
        .reset_index(drop=True)
    )


# ============================================================
# SIDEBAR
# ============================================================

st.sidebar.title("☀️ Smart Solar")
st.sidebar.caption("Generation monitoring and anomaly analysis")
st.sidebar.divider()

if st.sidebar.button("Refresh data now", use_container_width=True):
    st.cache_data.clear()
    st.rerun()

st.sidebar.caption("Source: Spark processed JSON files")
st.sidebar.caption(f"Automatic refresh: every {REFRESH_SECONDS} seconds")

# ============================================================
# AUTOMATIC REFRESH
# ============================================================

# Streamlit reruns the app periodically. The cache TTL ensures
# that the JSON files are reread instead of serving stale data.
@st.fragment(run_every=REFRESH_SECONDS)
def refresh_status():
    st.sidebar.caption(
        f"Last refresh check: {time.strftime('%H:%M:%S')}"
    )


refresh_status()

# ============================================================
# LOAD AND SELECT DATA
# ============================================================

df = load_data()

st.title("Smart Solar Analytics Dashboard")
st.write(
    "Monitor solar generation, environmental conditions, "
    "and potential low-generation anomalies."
)

if df.empty:
    st.warning(
        "No processed readings found. Start Kafka and Spark, "
        "then send readings to the solar-generation topic."
    )
    st.code("data/processed/solar_readings/")
    st.stop()

st.sidebar.subheader("Data View")

view_mode = st.sidebar.radio(
    "Choose records",
    [
        "Historical dataset",
        "Test and recent readings",
        "All readings",
    ],
)

# Temporary separation: the supplied historical dataset uses 2020.
if view_mode == "Historical dataset":
    display_df = df[df["DATE_TIME"].dt.year == 2020].copy()
elif view_mode == "Test and recent readings":
    display_df = df[df["DATE_TIME"].dt.year != 2020].copy()
else:
    display_df = df.copy()

if display_df.empty:
    st.info("No readings are available for the selected view.")
    st.stop()

display_df = (
    display_df.sort_values("DATE_TIME")
    .reset_index(drop=True)
)

latest = display_df.iloc[-1]

# ============================================================
# ANOMALY COUNTS
# ============================================================

low_flags = int(display_df["LOW_GENERATION_ANOMALY"].sum())
zero_flags = int(display_df["ZERO_GENERATION_ANOMALY"].sum())
quality_flags = int(display_df["DATA_QUALITY_ISSUE"].sum())

# ============================================================
# SYSTEM OVERVIEW
# ============================================================

st.subheader("System Overview")

c1, c2, c3, c4 = st.columns(4)

latest_ac = latest.get("AC_POWER")
latest_irradiation = latest.get("IRRADIATION")

c1.metric(
    "Latest AC Power",
    f"{latest_ac:,.2f}" if pd.notna(latest_ac) else "N/A",
)

c2.metric(
    "Latest Irradiation",
    f"{latest_irradiation:.3f}"
    if pd.notna(latest_irradiation) else "N/A",
)

c3.metric("Low-Generation Flags", low_flags)
c4.metric("Data Quality Issues", quality_flags)

st.caption(
    f"Distinct readings: {len(display_df):,} · "
    f"Zero-generation flags: {zero_flags:,} · "
    f"Latest timestamp: {latest['DATE_TIME']}"
)

# ============================================================
# GENERATION SUMMARY
# ============================================================

st.divider()
st.subheader("Generation Summary")

power_values = (
    display_df["AC_POWER"].dropna()
    if "AC_POWER" in display_df.columns
    else pd.Series(dtype=float)
)

g1, g2, g3 = st.columns(3)

if not power_values.empty:
    g1.metric("Average AC Power", f"{power_values.mean():,.2f}")
    g2.metric("Maximum AC Power", f"{power_values.max():,.2f}")
    g3.metric("Minimum AC Power", f"{power_values.min():,.2f}")
else:
    g1.metric("Average AC Power", "N/A")
    g2.metric("Maximum AC Power", "N/A")
    g3.metric("Minimum AC Power", "N/A")

st.caption(
    "Statistics describe power readings in the dataset's units; "
    "they are not energy totals."
)

# ============================================================
# ENVIRONMENTAL CONDITIONS
# ============================================================

st.divider()
st.subheader("Environmental Conditions")

left, right = st.columns(2)

with left:
    st.markdown("**Irradiation over time**")

    if "IRRADIATION" in display_df.columns:
        irradiation_data = (
            display_df[["DATE_TIME", "IRRADIATION"]]
            .dropna()
            .set_index("DATE_TIME")
        )

        if not irradiation_data.empty:
            st.line_chart(irradiation_data)
        else:
            st.info("No irradiation data available.")

with right:
    st.markdown("**Temperature over time**")

    temperature_columns = [
        column for column in [
            "AMBIENT_TEMPERATURE",
            "MODULE_TEMPERATURE",
        ]
        if column in display_df.columns
    ]

    if temperature_columns:
        temperature_data = (
            display_df[["DATE_TIME"] + temperature_columns]
            .dropna(subset=["DATE_TIME"])
            .set_index("DATE_TIME")
        )

        if not temperature_data.empty:
            st.line_chart(temperature_data)
        else:
            st.info("No temperature data available.")

# ============================================================
# SOLAR GENERATION AND DATE FILTER
# ============================================================

st.divider()
st.subheader("Solar Generation")

valid_dates = display_df["DATE_TIME"].dropna()

if not valid_dates.empty:
    min_date = valid_dates.min().date()
    max_date = valid_dates.max().date()

    selected_dates = st.date_input(
        "Filter generation by date",
        value=(min_date, max_date),
        min_value=min_date,
        max_value=max_date,
        key="generation_date_filter",
    )

    if isinstance(selected_dates, (tuple, list)):
        if len(selected_dates) == 2:
            start_date, end_date = selected_dates
        elif len(selected_dates) == 1:
            start_date = end_date = selected_dates[0]
        else:
            start_date, end_date = min_date, max_date
    else:
        start_date = end_date = selected_dates

    chart_df = display_df[
        display_df["DATE_TIME"].dt.date.between(
            start_date,
            end_date,
        )
    ].copy()

    power_columns = [
        column for column in ["AC_POWER", "DC_POWER"]
        if column in chart_df.columns
    ]

    if not chart_df.empty and power_columns:
        power_data = (
            chart_df[["DATE_TIME"] + power_columns]
            .dropna(subset=["DATE_TIME"])
            .set_index("DATE_TIME")
        )

        st.line_chart(power_data)

        st.caption(
            f"Showing {len(chart_df):,} readings "
            f"from {start_date} to {end_date}."
        )
    else:
        st.info("No generation readings for the selected dates.")

# ============================================================
# POTENTIAL ANOMALIES AND FILTERS
# ============================================================

st.divider()
st.subheader("Potential Anomalies")

flagged = display_df[
    display_df["LOW_GENERATION_ANOMALY"]
].copy()

if flagged.empty:
    st.success("No low-generation flags in the selected records.")
else:
    flagged["POWER_SHORTFALL"] = (
        flagged["EXPECTED_MIN_AC_POWER"] - flagged["AC_POWER"]
    ).clip(lower=0)

    st.markdown("**Filter flagged readings**")

    f1, f2, f3 = st.columns(3)

    with f1:
        max_irradiation = (
            float(flagged["IRRADIATION"].max())
            if flagged["IRRADIATION"].notna().any()
            else 2.0
        )

        min_irradiation = st.number_input(
            "Minimum irradiation",
            min_value=0.0,
            max_value=max(0.0, max_irradiation),
            value=0.0,
            step=0.05,
        )

    with f2:
        min_shortfall = st.number_input(
            "Minimum power shortfall",
            min_value=0.0,
            value=0.0,
            step=100.0,
        )

    with f3:
        valid_flag_dates = flagged["DATE_TIME"].dropna()

        if not valid_flag_dates.empty:
            anomaly_dates = st.date_input(
                "Anomaly date range",
                value=(
                    valid_flag_dates.min().date(),
                    valid_flag_dates.max().date(),
                ),
                min_value=valid_flag_dates.min().date(),
                max_value=valid_flag_dates.max().date(),
                key="anomaly_date_filter",
            )
        else:
            anomaly_dates = ()

    filtered = flagged[
        (flagged["IRRADIATION"] >= min_irradiation)
        & (flagged["POWER_SHORTFALL"] >= min_shortfall)
    ].copy()

    if isinstance(anomaly_dates, (tuple, list)):
        if len(anomaly_dates) == 2:
            start_date, end_date = anomaly_dates
            filtered = filtered[
                filtered["DATE_TIME"].dt.date.between(
                    start_date,
                    end_date,
                )
            ]
        elif len(anomaly_dates) == 1:
            filtered = filtered[
                filtered["DATE_TIME"].dt.date == anomaly_dates[0]
            ]

    st.warning(
        f"{len(filtered):,} of {len(flagged):,} flagged readings "
        "match the selected filters."
    )

    a1, a2, a3 = st.columns(3)

    a1.metric("Matching Readings", f"{len(filtered):,}")
    a2.metric(
        "Average Power Shortfall",
        f"{filtered['POWER_SHORTFALL'].mean():,.2f}"
        if not filtered.empty else "N/A",
    )
    a3.metric(
        "Maximum Power Shortfall",
        f"{filtered['POWER_SHORTFALL'].max():,.2f}"
        if not filtered.empty else "N/A",
    )

    anomaly_columns = [
        column for column in [
            "DATE_TIME",
            "IRRADIATION",
            "AC_POWER",
            "EXPECTED_MIN_AC_POWER",
            "POWER_SHORTFALL",
        ]
        if column in filtered.columns
    ]

    st.dataframe(
        filtered[anomaly_columns].sort_values(
            "DATE_TIME",
            ascending=False,
        ),
        use_container_width=True,
        hide_index=True,
    )

    st.download_button(
        "Download filtered anomalies (CSV)",
        data=filtered[anomaly_columns].to_csv(index=False),
        file_name="solar_anomalies.csv",
        mime="text/csv",
    )

st.caption(
    "Power shortfall is the difference between the selected historical "
    "threshold and actual AC power. It is not measured lost energy. "
    "Anomaly flags are exploratory indicators, not confirmed equipment faults."
)

# ============================================================
# PROCESSED READINGS
# ============================================================

with st.expander("View processed readings"):
    st.dataframe(
        display_df.sort_values(
            "DATE_TIME",
            ascending=False,
        ),
        use_container_width=True,
        hide_index=True,
    )

# ============================================================
# FOOTER
# ============================================================

st.divider()
st.caption(
    "Smart Solar Analytics | Kafka + Spark Structured Streaming + Streamlit"
)
