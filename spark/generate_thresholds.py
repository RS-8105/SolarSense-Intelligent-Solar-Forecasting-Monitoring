
import json
import pandas as pd
from pathlib import Path

DATA_DIR = Path("data/raw/Solar Power Generation")
OUTPUT_FILE = Path("models/solar_anomaly_thresholds.json")

# Load historical generation and weather data
generation = pd.read_csv(
    DATA_DIR / "Plant_1_Generation_Data.csv"
)
weather = pd.read_csv(
    DATA_DIR / "Plant_1_Weather_Sensor_Data.csv"
)

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

# Aggregate inverter power to plant-level power
plant_generation = (
    generation.groupby("DATE_TIME", as_index=False)
    .agg(AC_POWER=("AC_POWER", "sum"))
)

weather_features = weather[
    ["DATE_TIME", "IRRADIATION"]
].drop_duplicates(subset=["DATE_TIME"])

data = plant_generation.merge(
    weather_features,
    on="DATE_TIME",
    how="inner",
)

# Keep readings with meaningful irradiation
data = data[data["IRRADIATION"] > 0.1].copy()

# Define irradiation ranges
bins = [0.1, 0.3, 0.5, 0.7, 0.9, float("inf")]
labels = ["0.1-0.3", "0.3-0.5", "0.5-0.7", "0.7-0.9", ">0.9"]

data["SUNLIGHT_RANGE"] = pd.cut(
    data["IRRADIATION"],
    bins=bins,
    labels=labels,
    include_lowest=True,
)

# Calculate the 10th-percentile power threshold per range
thresholds = {}

for sunlight_range, group in data.groupby(
    "SUNLIGHT_RANGE", observed=True
):
    thresholds[str(sunlight_range)] = round(
        float(group["AC_POWER"].quantile(0.10)), 2
    )

# Save thresholds for Spark to load later
OUTPUT_FILE.parent.mkdir(parents=True, exist_ok=True)

with OUTPUT_FILE.open("w", encoding="utf-8") as file:
    json.dump(thresholds, file, indent=2)

print(f"Saved thresholds to: {OUTPUT_FILE}")
print(f"Historical daylight readings: {len(data)}")
print("\nThresholds:")

for sunlight_range, threshold in thresholds.items():
    print(f"{sunlight_range}: {threshold}")
