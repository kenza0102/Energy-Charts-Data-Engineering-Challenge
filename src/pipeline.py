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


def bronze_to_silver_public_power(spark: SparkSession): #Public power -> silver
    bronze = spark.read.format("delta").load(os.path.join(BRONZE_DIR, "public_power"))
    silver = bronze.select("day", "ingestion_ts", "raw_json")
    write_delta(silver, os.path.join(SILVER_DIR, "public_power"), mode="overwrite")


def bronze_to_silver_price(spark: SparkSession): #Price -> silver
    bronze = spark.read.format("delta").load(os.path.join(BRONZE_DIR, "price"))
    silver = bronze.select("day", "ingestion_ts", "raw_json")
    write_delta(silver, os.path.join(SILVER_DIR, "price"), mode="overwrite")

### Gold (meta table)
def silver_to_gold(spark: SparkSession):
    silver_power = spark.read.format("delta").load(os.path.join(SILVER_DIR, "public_power"))
    silver_price = spark.read.format("delta").load(os.path.join(SILVER_DIR, "price"))

    gold_meta = (
        silver_power.groupBy()
        .agg(F.count("*").alias("power_rows"))
        .crossJoin(silver_price.groupBy().agg(F.count("*").alias("price_rows")))
        .withColumn("built_ts", F.current_timestamp())
    )

    write_delta(gold_meta, os.path.join(GOLD_DIR, "meta"), mode="overwrite")

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

