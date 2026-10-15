
import pandas as pd

DATA_DIR = "data/raw/Solar Power Generation/"

# Load historical data
generation = pd.read_csv(
    DATA_DIR + "Plant_1_Generation_Data.csv"
)
weather = pd.read_csv(
    DATA_DIR + "Plant_1_Weather_Sensor_Data.csv"
)

# Parse timestamps
generation["DATE_TIME"] = pd.to_datetime(
    generation["DATE_TIME"],
    format="%d-%m-%Y %H:%M",
    errors="coerce",
)
weather["DATE_TIME"] = pd.to_datetime(
    weather["DATE_TIME"],
    errors="coerce",
)

generation = generation.dropna(subset=["DATE_TIME"])
weather = weather.dropna(subset=["DATE_TIME"])

# Aggregate inverter readings into plant-level power
plant_generation = (
    generation.groupby("DATE_TIME", as_index=False)
    .agg(
        AC_POWER=("AC_POWER", "sum"),
        DC_POWER=("DC_POWER", "sum"),
    )
)

weather_features = weather[
    [
        "DATE_TIME",
        "AMBIENT_TEMPERATURE",
        "MODULE_TEMPERATURE",
        "IRRADIATION",
    ]
].drop_duplicates(subset=["DATE_TIME"])

# Match power readings with weather readings
data = pd.merge(
    plant_generation,
    weather_features,
    on="DATE_TIME",
    how="inner",
).sort_values("DATE_TIME")

# Apply the same rules used by Spark
data["DATA_QUALITY_ISSUE"] = (
    (data["AC_POWER"] < 0)
    | (data["DC_POWER"] < 0)
    | (data["IRRADIATION"] < 0)
)

data["LOW_GENERATION_ANOMALY"] = (
    (data["IRRADIATION"] > 0.5)
    & (data["AC_POWER"] == 0)
)

# Report results
print("\n===== HISTORICAL ANOMALY EVALUATION =====")
print(f"Total matched readings: {len(data)}")

print(
    "\nData quality issues:",
    int(data["DATA_QUALITY_ISSUE"].sum()),
)

print(
    "Low-generation anomalies:",
    int(data["LOW_GENERATION_ANOMALY"].sum()),
)

print("\nSuspicious readings:")
print(
    data.loc[
        data["LOW_GENERATION_ANOMALY"],
        [
            "DATE_TIME",
            "AC_POWER",
            "IRRADIATION",
            "LOW_GENERATION_ANOMALY",
        ],
    ].head(10).to_string(index=False)
)

print("\nEvaluation complete.")
