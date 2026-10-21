
import json
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    from_json,
    to_timestamp,
    when,
    lit,
    current_timestamp,
)
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    DoubleType,
)

# ---------------- CONFIGURATION ----------------

KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
KAFKA_TOPIC = "solar-generation"

THRESHOLD_FILE = Path("models/solar_anomaly_thresholds.json")
OUTPUT_PATH = "data/processed/solar_readings_live"
CHECKPOINT_PATH = "/tmp/smart-solar-spark-checkpoint-v6"

# ---------------- LOAD THRESHOLDS ----------------

with THRESHOLD_FILE.open("r", encoding="utf-8") as file:
    THRESHOLDS = json.load(file)

print("Loaded historical anomaly thresholds:")
for sunlight_range, threshold in THRESHOLDS.items():
    print(f"  {sunlight_range}: {threshold}")

# ---------------- MESSAGE SCHEMA ----------------

schema = StructType([
    StructField("DATE_TIME", StringType(), True),
    StructField("PLANT_ID", StringType(), True),
    StructField("AC_POWER", DoubleType(), True),
    StructField("DC_POWER", DoubleType(), True),
    StructField("DAILY_YIELD", DoubleType(), True),
    StructField("TOTAL_YIELD", DoubleType(), True),
    StructField("AMBIENT_TEMPERATURE", DoubleType(), True),
    StructField("MODULE_TEMPERATURE", DoubleType(), True),
    StructField("IRRADIATION", DoubleType(), True),
])

# ---------------- SPARK SESSION ----------------

spark = (
    SparkSession.builder
    .appName("SmartSolarKafkaConsumer")
    .master("local[*]")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

print(f"Connecting to Kafka topic: {KAFKA_TOPIC}")

# ---------------- KAFKA STREAM ----------------

raw_stream = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
    .option("subscribe", KAFKA_TOPIC)
    .option("startingOffsets", "latest")
    .load()
)

parsed_stream = (
    raw_stream
    .selectExpr("CAST(value AS STRING) AS json_value")
    .select(from_json(col("json_value"), schema).alias("data"))
    .select("data.*")
    .filter(col("DATE_TIME").isNotNull())
    .withColumn("EVENT_TIME", to_timestamp(col("DATE_TIME")))
    .withColumn("INGESTION_TIME", current_timestamp())
)

# ---------------- ANOMALY DETECTION ----------------

# Check for negative power or irradiation values.
data_quality_issue = (
    (col("AC_POWER") < 0)
    | (col("DC_POWER") < 0)
    | (col("IRRADIATION") < 0)
)

# Flag zero AC power only when meaningful irradiation is available.
zero_generation_anomaly = (
    col("IRRADIATION").isNotNull()
    & (col("IRRADIATION") > 0.5)
    & (col("AC_POWER") == 0)
)

# Select a threshold based on the irradiation range.
generation_threshold = (
    when(
        (col("IRRADIATION") > 0.1)
        & (col("IRRADIATION") <= 0.3),
        lit(THRESHOLDS["0.1-0.3"]),
    )
    .when(
        (col("IRRADIATION") > 0.3)
        & (col("IRRADIATION") <= 0.5),
        lit(THRESHOLDS["0.3-0.5"]),
    )
    .when(
        (col("IRRADIATION") > 0.5)
        & (col("IRRADIATION") <= 0.7),
        lit(THRESHOLDS["0.5-0.7"]),
    )
    .when(
        (col("IRRADIATION") > 0.7)
        & (col("IRRADIATION") <= 0.9),
        lit(THRESHOLDS["0.7-0.9"]),
    )
    .when(
        col("IRRADIATION") > 0.9,
        lit(THRESHOLDS[">0.9"]),
    )
)

# Only evaluate low generation when irradiation is available.
low_generation_anomaly = (
    col("IRRADIATION").isNotNull()
    & (col("IRRADIATION") > 0.1)
    & (col("AC_POWER") < generation_threshold)
)

processed_stream = (
    parsed_stream
    .withColumn(
        "DATA_QUALITY_ISSUE",
        when(data_quality_issue, True).otherwise(False),
    )
    .withColumn(
        "ZERO_GENERATION_ANOMALY",
        when(zero_generation_anomaly, True).otherwise(False),
    )
    .withColumn(
        "EXPECTED_MIN_AC_POWER",
        generation_threshold,
    )
    .withColumn(
        "LOW_GENERATION_ANOMALY",
        when(low_generation_anomaly, True).otherwise(False),
    )
)
# ---------------- PERSISTENT JSON OUTPUT ----------------

Path(OUTPUT_PATH).mkdir(parents=True, exist_ok=True)

query = (
    processed_stream.writeStream
    .format("json")
    .outputMode("append")
    .option("path", OUTPUT_PATH)
    .option("checkpointLocation", CHECKPOINT_PATH)
    .start()
)

print("Spark streaming started successfully.")
print(f"Topic: {KAFKA_TOPIC}")
print(f"Output directory: {OUTPUT_PATH}")
print("Send new Kafka readings to test the output.")
print("Press Ctrl+C to stop.")

try:
    query.awaitTermination()
except KeyboardInterrupt:
    print("Stopping Spark streaming query...")
    query.stop()
finally:
    spark.stop()
