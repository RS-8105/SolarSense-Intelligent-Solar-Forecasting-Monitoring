
import json
import time
import pandas as pd
from kafka import KafkaProducer

# Configuration
DATA_DIR = "data/raw/Solar Power Generation/"
TOPIC = "solar-generation"
BOOTSTRAP_SERVERS = "localhost:9092"
DELAY_SECONDS = 0.2


def main():
    print("Loading Plant 1 solar datasets...")

    # Load datasets
    generation = pd.read_csv(
        DATA_DIR + "Plant_1_Generation_Data.csv"
    )
    weather = pd.read_csv(
        DATA_DIR + "Plant_1_Weather_Sensor_Data.csv"
    )

    # Normalize timestamps
    generation["DATE_TIME"] = pd.to_datetime(
        generation["DATE_TIME"],
        format="%d-%m-%Y %H:%M",
        errors="coerce",
    )

    weather["DATE_TIME"] = pd.to_datetime(
        weather["DATE_TIME"],
        errors="coerce",
    )

    # Remove invalid timestamps
    generation = generation.dropna(subset=["DATE_TIME"])
    weather = weather.dropna(subset=["DATE_TIME"])

    # Aggregate inverter readings into plant-level metrics
    plant_generation = (
        generation.groupby("DATE_TIME", as_index=False)
        .agg(
            DC_POWER=("DC_POWER", "sum"),
            AC_POWER=("AC_POWER", "sum"),
            DAILY_YIELD=("DAILY_YIELD", "sum"),
            TOTAL_YIELD=("TOTAL_YIELD", "sum"),
        )
    )

    # Select weather features
    weather_features = weather[
        [
            "DATE_TIME",
            "AMBIENT_TEMPERATURE",
            "MODULE_TEMPERATURE",
            "IRRADIATION",
        ]
    ].drop_duplicates(subset=["DATE_TIME"])

    # Merge generation and weather on timestamp
    merged_data = pd.merge(
        plant_generation,
        weather_features,
        on="DATE_TIME",
        how="inner",
    )

    # Sort chronologically
    merged_data = merged_data.sort_values("DATE_TIME")

    print(f"Matched readings: {len(merged_data)}")
    print("Connecting to Kafka...")

    producer = KafkaProducer(
        bootstrap_servers=BOOTSTRAP_SERVERS,
        value_serializer=lambda value: json.dumps(value).encode("utf-8"),
        acks="all",
        retries=3,
    )

    sent_count = 0

    try:
        for _, row in merged_data.iterrows():
            message = {
                "DATE_TIME": row["DATE_TIME"].strftime("%Y-%m-%d %H:%M:%S"),
                "DC_POWER": float(row["DC_POWER"]),
                "AC_POWER": float(row["AC_POWER"]),
                "DAILY_YIELD": float(row["DAILY_YIELD"]),
                "TOTAL_YIELD": float(row["TOTAL_YIELD"]),
                "AMBIENT_TEMPERATURE": float(row["AMBIENT_TEMPERATURE"]),
                "MODULE_TEMPERATURE": float(row["MODULE_TEMPERATURE"]),
                "IRRADIATION": float(row["IRRADIATION"]),
            }

            producer.send(TOPIC, value=message).get(timeout=30)
            sent_count += 1

            if sent_count % 50 == 0:
                print(f"Sent {sent_count} readings...")

            time.sleep(DELAY_SECONDS)

        producer.flush()
        print(f"Streaming complete! Total readings sent: {sent_count}")

    finally:
        producer.close()


if __name__ == "__main__":
    main()
