import os
import json
import requests
from datetime import datetime, date, timedelta
from typing import Optional

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from delta.tables import DeltaTable

"""
Energy-Charts pipeline (local):
Bronze (raw JSON) -> Silver (lightly structured) -> Gold (basic aggregate / meta)

- Ingestes data day-by-day from https://api.energy-charts.info
- Stores outputs as Delta Lake tables
- Safe reruns (idempotent) by deleting existing day before inserting
"""

DELTA_VERSION = "4.0.1"
IVY_DIR = os.path.join(os.getcwd(), ".ivy2_spark")
os.makedirs(IVY_DIR, exist_ok=True)


BRONZE_DIR = "data/bronze"
SILVER_DIR = "data/silver"
GOLD_DIR = "data/gold"

BASE_URL = "https://api.energy-charts.info"


## Spark / Delta configuration
def create_spark() -> SparkSession:
    spark = (
        SparkSession.builder
        .appName("energy-charts-pipeline")
        .master("local[*]")
        .config("spark.jars.packages", f"io.delta:delta-spark_2.13:{DELTA_VERSION}")
        .config("spark.jars.ivy", IVY_DIR)
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark

### Helpers

def daterange(start: date, end: date): # Yield all dates from start to end
    d = start
    while d <= end:
        yield d
        d += timedelta(days=1)


def fetch_json(endpoint: str, params: dict) -> dict: #Download JSON from the the Energy-Charts API
    r = requests.get(f"{BASE_URL}/{endpoint}", params=params, timeout=60)
    r.raise_for_status()
    return r.json()

def write_delta(df, path: str, mode: str = "append", partition_by: Optional[str] = None): #Write dataframe as a Delta table
    writer = df.write.format("delta").mode(mode)
    if partition_by:
        writer = writer.partitionBy(partition_by)
    writer.save(path)


def delete_day_if_exists(spark: SparkSession, table_path: str, day_str: str): #Idempotency helper: delete one day if it already exists
    if os.path.exists(os.path.join(table_path, "_delta_log")):
        dt = DeltaTable.forPath(spark, table_path)
        dt.delete(F.col("day") == day_str)



### Bronze ingestion (raw data layer)

def ingest_power_bronze(spark: SparkSession, day: date, country: str = "de"): #Ingest public power (raw)
    data = fetch_json("public_power", {"country": country, "date": day.isoformat()})

    raw = (
        spark.createDataFrame([(json.dumps(data),)], ["raw_json"])
        .withColumn("ingestion_ts", F.current_timestamp())
        .withColumn("day", F.lit(day.isoformat()))
        .withColumn("dataset", F.lit("public_power"))
    )

    table_path = os.path.join(BRONZE_DIR, "public_power")
    delete_day_if_exists(spark, table_path, day.isoformat())

    write_delta(raw, table_path, mode="append", partition_by="day")


def ingest_price_bronze(spark: SparkSession, day: date, zone: str = "DE-LU"): #Ingest price (raw)
    data = fetch_json("price", {"bzn": zone, "date": day.isoformat()})

    raw = (
        spark.createDataFrame([(json.dumps(data),)], ["raw_json"])
        .withColumn("ingestion_ts", F.current_timestamp())
        .withColumn("day", F.lit(day.isoformat()))
        .withColumn("dataset", F.lit("price"))
    )

    table_path = os.path.join(BRONZE_DIR, "price")
    delete_day_if_exists(spark, table_path, day.isoformat())

    write_delta(raw, table_path, mode="append", partition_by="day")

### Silver (placeholder for now)


def bronze_to_silver_public_power(spark: SparkSession):
    bronze = spark.read.format("delta").load(
        os.path.join(BRONZE_DIR, "public_power")
    )

    # Parse raw JSON
    parsed = bronze.withColumn(
        "json",
        F.from_json(
            "raw_json",
            """
            STRUCT<
              unix_seconds: ARRAY<LONG>,
              production_types: ARRAY<
                STRUCT<
                  name: STRING,
                  data: ARRAY<DOUBLE>
                >
              >
            >
            """
        )
    )

    # Explode production types
    exploded_types = parsed.withColumn(
        "production_type",
        F.explode("json.production_types")
    )

    # Zip timestamps with values
    zipped = exploded_types.withColumn(
        "ts_value_pairs",
        F.arrays_zip(
            "json.unix_seconds",
            "production_type.data"
        )
    )

    # Explode timestamp-value pairs
    exploded_rows = zipped.withColumn(
        "ts_value",
        F.explode("ts_value_pairs")
    )

    silver = exploded_rows.select(
        F.col("day"),
        F.to_timestamp(F.col("ts_value.unix_seconds")).alias("timestamp"),
        F.col("production_type.name").alias("production_type"),
        F.col("ts_value.data").alias("power_mw"),
        F.col("ingestion_ts")
    )

    write_delta(
        silver,
        os.path.join(SILVER_DIR, "public_power"),
        mode="overwrite",
        partition_by="day"
    )


def bronze_to_silver_price(spark: SparkSession):
    bronze = spark.read.format("delta").load(
        os.path.join(BRONZE_DIR, "price")
    )

    # Parse raw JSON
    parsed = bronze.withColumn(
        "json",
        F.from_json(
            "raw_json",
            """
            STRUCT<
              unix_seconds: ARRAY<LONG>,
              price: ARRAY<DOUBLE>
            >
            """
        )
    )

    # Zip timestamps with prices
    zipped = parsed.withColumn(
        "ts_price_pairs",
        F.arrays_zip(
            "json.unix_seconds",
            "json.price"
        )
    )

    # Explode into event-level rows
    exploded = zipped.withColumn(
        "ts_price",
        F.explode("ts_price_pairs")
    )

    silver = exploded.select(
        F.col("day"),
        F.to_timestamp(F.col("ts_price.unix_seconds")).alias("timestamp"),
        F.col("ts_price.price").alias("price_eur_mwh"),
        F.col("ingestion_ts")
    )

    write_delta(
        silver,
        os.path.join(SILVER_DIR, "price"),
        mode="overwrite",
        partition_by="day"
    )

### Gold (meta table)
def silver_to_gold(spark: SparkSession):
    silver_power = spark.read.format("delta").load(
        os.path.join(SILVER_DIR, "public_power")
    )
    silver_price = spark.read.format("delta").load(
        os.path.join(SILVER_DIR, "price")
    )

    # ----------------------------
    # Gold: daily public power
    # ----------------------------
    gold_daily_power = (
        silver_power
        .groupBy("day", "production_type")
        .agg(
            F.sum("power_mw").alias("daily_net_power_mwh")
        )
        .withColumn("built_ts", F.current_timestamp())
    )

    # ----------------------------
    # Gold: daily price
    # ----------------------------
    gold_daily_price = (
        silver_price
        .groupBy("day")
        .agg(
            F.avg("price_eur_mwh").alias("avg_price_eur_mwh")
        )
        .withColumn("built_ts", F.current_timestamp())
    )

    write_delta(
        gold_daily_power,
        os.path.join(GOLD_DIR, "daily_public_power"),
        mode="overwrite",
        partition_by="day"
    )

    write_delta(
        gold_daily_price,
        os.path.join(GOLD_DIR, "daily_price"),
        mode="overwrite",
        partition_by="day"
    )

### Orchestration
def run(start: str, end: str, country: str = "de", zone: str = "DE-LU"):
    print(" PIPELINE RUNNING: bronze -> silver -> gold")
    print("start:", start, "end:", end, "country:", country, "zone:", zone)

    spark = create_spark()

    start_d = datetime.fromisoformat(start).date()
    end_d = datetime.fromisoformat(end).date()

    try:
        for d in daterange(start_d, end_d):
            print("→ ingesting day:", d.isoformat())
            ingest_power_bronze(spark, d, country=country)
            ingest_price_bronze(spark, d, zone=zone)

        print("→ transforming to silver")
        bronze_to_silver_public_power(spark)
        bronze_to_silver_price(spark)

        print("→ building gold")
        silver_to_gold(spark)

        print("DONE")
    finally:
        spark.stop()

