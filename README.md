# Energy-Charts-Data-Engineering-Challenge

This project implements a local Bronze → Silver → Gold data pipeline using PySpark and Delta Lake.

The pipeline ingests daily electricity production and price data from the Energy-Charts public API, stores raw data safely, transforms it into structured event-level tables, and finally produces daily analytical aggregates.

The objective is to demonstrate a reproducible, idempotent data engineering workflow that can be run locally end-to-end.


## Challenge description

The challenge was to build a small data pipeline that:

- fetches daily energy data from a public API,
- stores it reliably,
- transforms it step-by-step into structured datasets,
- produces analytics-ready outputs,
- supports safe reruns and backfilling.

## Data source

Data is fetched from the public **Energy-Charts API**:

- Base URL: `https://api.energy-charts.info`
- Endpoints used:
  - `public_power`  electricity production by energy source
  - `price`  electricity market price

Data is fetched day by day, which enables:
- historical backfilling,
- partial reruns,
- deterministic and reproducible results.

## Architecture: Bronze -> Silver -> Gold

This project follows the medallion architecture, a standard pattern in data engineering.

### Bronze layer (raw ingestion)
Purpose: 
- Stores the raw API response exactly as received.
- Preserves original JSON for future reprocessing.
- Guarantee safe reruns.

Implementation:
- One record per day per dataset
- Raw JSON stored as a string
- Partitioned by day
- Idempotent: existing data for a given day is deleted before insertion


Tables:
- `data/bronze/public_power`
- `data/bronze/price`

Schema:
- `raw_json` raw API response (string)
- `ingestion_ts` ingestion timestamp
- `day` ingestion date (YYYY-MM-DD)
- `dataset` dataset name


### Silver layer (Structured event data)
Purpose:
- Parse raw JSON
- Normalize time-series data
- Produce event-level, analytics-ready rows

#### Public Power (Silver)
- Parses production types and time series
- Explodes timestamps and values
- Produces one row per:
    - timestamp
    - production type
    - power value

Table:
- `data/silver/public_power`

Schema:
- `day` 
- `timestamp`
- `production_type` 
- `power_mw` 
- `ingestion_ts` 

#### Price (Silver)
- Parses price time series
- Explodes timestamps and prices
- Produces one row per timestamp

Table:
- `data/silver/price`
Schema:
- `day` 
- `timestamp`
- `price_eur_mwh` 
- `ingestion_ts` 


### Gold layer (Daily aggregates)
Purpose:
- Provide analytics-ready daily summaries
- Validate the full pipeline end-to-end

#### Daily public power:
Aggregates total daily energy production per energy source.

Tables:
- `data/gold/daily_public_power`

Metrics:
 `daily_net_power_mwh` sum of power values per day and production type.

#### Daily electricity price
Computes the average daily electricity price.
Table:
- `data/gold/daily_price`
Metrics:
- `avg_price_eur_mwh` daily average price

## Project structure

```text
energy-charts-pipeline/
├── main.py                  # Pipeline entry point
├── src/
│   └── pipeline.py          # Spark configuration, ingestion, transformations
├── requirements.txt         # Python dependencies
├── README.md                # Project documentation
├── .gitignore               # Files excluded from git
└── data/                    # Generated locally (not committed)
    ├── bronze/
    ├── silver/
    └── gold/
```
##  Setup instructions
Prerequisites: 
- Python 3.9 or newer
- Java JDK 17 (required by Apache Spark)

To verify Java installation:

```bash
java -version
```

To create and activate a virtual environment:
```bash
python3 -m venv .venv
source .venv/bin/activate
```

To install dependencies:
```bash
pip install -r requirements.txt
```
To execute the pipeline run :
```bash
python3 main.py
```
This command will:
- Fetch daily power production and price data from the Energy-Charts API
- Store raw JSON data in Bronze Delta tables
- Transform raw data into Silver event-level tables
- Build Gold daily aggregates
- Stop Spark cleanly after completion

The date range, country, and price zone can be adjusted directly in `main.py`.

## Idempotency and safe reruns
The pipeline is designed to be safe to rerun.

For each day:
- Existing records for that day are deleted from Bronze
- Fresh data is then inserted

This guarantees:
- no duplicate records
- reliable backfilling
- deterministic results
This behavior mirrors real-world production pipelines.

## Validation
After a successful run, each Delta table contains a `_delta_log` directory:
```text
data/bronze/public_power/_delta_log
data/bronze/price/_delta_log
data/silver/public_power/_delta_log
data/silver/price/_delta_log
data/gold/daily_public_power/_delta_log
data/gold/daily_price/_delta_log
```
The presence of `_delta_log` confirms correct Delta Lake storage and transaction tracking.

## Design decisions
- Delta Lake was chosen for reliable storage, versioning, and safe overwrites.
- Bronze stores raw JSON to allow reprocessing if API schemas change.
- Silver normalizes time-series data into event-level rows.
- Gold focuses on daily aggregates to validate analytical readiness
- Pipeline runs locally in `local[*]` mode for simplicity and portability

## Assumptions and limitations

- Designed for local execution (not distributed cluster deployment)
- No orchestration tool (Airflow, Dagster) — single-script execution
- Aggregations are intentionally simple to demonstrate structure rather than advanced analytics