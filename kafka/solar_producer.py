import json
import time
import pandas as pd
from kafka import KafkaProducer

# ---------------- CONFIGURATION ----------------

DATA_DIR = "data/raw/Solar Power Generation/"
TOPIC = "solar-generation"
BOOTSTRAP_SERVERS = "localhost:9092"
DELAY_SECONDS = 0.2


def load_plant_data(plant_number):
    print(f"Loading Plant {plant_number} datasets...")

    generation = pd.read_csv(
        DATA_DIR + f"Plant_{plant_number}_Generation_Data.csv"
    )

    weather = pd.read_csv(
        DATA_DIR + f"Plant_{plant_number}_Weather_Sensor_Data.csv"
    )

    generation["DATE_TIME"] = pd.to_datetime(
        generation["DATE_TIME"],
        errors="coerce",
        dayfirst=False,
    )

    weather["DATE_TIME"] = pd.to_datetime(
        weather["DATE_TIME"],
        errors="coerce",
    )

    generation = generation.dropna(subset=["DATE_TIME"])
    weather = weather.dropna(subset=["DATE_TIME"])

    # Aggregate inverter readings at each timestamp.
    plant_generation = (
        generation.groupby(["PLANT_ID", "DATE_TIME"], as_index=False)
        .agg(
            DC_POWER=("DC_POWER", "sum"),
            AC_POWER=("AC_POWER", "sum"),
            DAILY_YIELD=("DAILY_YIELD", "sum"),
            TOTAL_YIELD=("TOTAL_YIELD", "sum"),
        )
    )

    # One weather reading per timestamp.
    weather_features = weather[
        [
            "DATE_TIME",
            "AMBIENT_TEMPERATURE",
            "MODULE_TEMPERATURE",
            "IRRADIATION",
        ]
    ].drop_duplicates(subset=["DATE_TIME"])

    # Keep generation records even if weather data is unavailable.
    merged_data = pd.merge(
        plant_generation,
        weather_features,
        on="DATE_TIME",
        how="left",
    ).sort_values("DATE_TIME")

    print(f"Plant {plant_number}: {len(merged_data)} matched readings")

    return merged_data


def main():
    plant_1 = load_plant_data(1)
    plant_2 = load_plant_data(2)

    # Combine both plants and replay chronologically.
    merged_data = pd.concat(
        [plant_1, plant_2],
        ignore_index=True,
    ).sort_values("DATE_TIME")

    print(f"Total readings per replay cycle: {len(merged_data)}")
    print("Connecting to Kafka...")

    producer = KafkaProducer(
        bootstrap_servers=BOOTSTRAP_SERVERS,
        value_serializer=lambda value: json.dumps(value).encode("utf-8"),
        acks="all",
        retries=3,
    )

    sent_count = 0
    cycle = 1

    try:
        while True:
            print(f"\nStarting replay cycle {cycle}...")

            for _, row in merged_data.iterrows():
                message = {
                    "DATE_TIME": row["DATE_TIME"].strftime(
                        "%Y-%m-%d %H:%M:%S"
                    ),
                    "PLANT_ID": str(row["PLANT_ID"]),
                    "DC_POWER": float(row["DC_POWER"]),
                    "AC_POWER": float(row["AC_POWER"]),
                    "DAILY_YIELD": float(row["DAILY_YIELD"]),
                    "TOTAL_YIELD": float(row["TOTAL_YIELD"]),
                    "AMBIENT_TEMPERATURE": (
                        float(row["AMBIENT_TEMPERATURE"])
                        if pd.notna(row["AMBIENT_TEMPERATURE"])
                        else None
                    ),
                    "MODULE_TEMPERATURE": (
                        float(row["MODULE_TEMPERATURE"])
                        if pd.notna(row["MODULE_TEMPERATURE"])
                        else None
                    ),
                    "IRRADIATION": (
                        float(row["IRRADIATION"])
                        if pd.notna(row["IRRADIATION"])
                        else None
                    ),
                }

                producer.send(TOPIC, value=message).get(timeout=30)
                sent_count += 1

                if sent_count % 1000 == 0:
                    print(
                        f"Cycle {cycle}: sent {sent_count} total readings"
                    )

                time.sleep(DELAY_SECONDS)

            producer.flush()
            print(f"Cycle {cycle} complete. Total sent: {sent_count}")
            cycle += 1

    except KeyboardInterrupt:
        print(f"\nStopping producer. Total readings sent: {sent_count}")

    finally:
        producer.close()


if __name__ == "__main__":
    main()