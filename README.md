# Energy-Charts-Data-Engineering-Challenge

This project implements a local Bronze → Silver → Gold data pipeline using PySpark and Delta Lake.  
The pipeline ingests electricity production and price data from the Energy-Charts API, stores it as Delta tables, and prepares it for analytics.

The goal of this project is to demonstrate a reproducible data engineering workflow with proper storage, structure, and safe reruns, as required by the technical challenge.


## Challenge description

The challenge was to build a small data pipeline that:

- fetches daily energy data from a public API,
- stores it reliably,
- organizes it step by step for analysis,
- and can be run locally by anyone.

## Data source

Data is fetched from the public **Energy-Charts API**:

- Base URL: `https://api.energy-charts.info`
- Endpoints used:
  - `public_power` — electricity production
  - `price` — electricity price

The pipeline fetches data one day at a time, which enables:
- backfilling historical data,
- safe reruns,
- reproducible results.

## Architecture: Bronze -> Silver -> Gold

This project follows the medallion architecture, a standard pattern in data engineering.

### Bronze layer (raw data)

- Stores the raw API response as JSON.
- No transformation or cleaning is applied.
- Preserves the original source data exactly as received.

Tables:
- `data/bronze/public_power`
- `data/bronze/price`

Schema:
- `raw_json`
- `ingestion_ts`
- `day`
- `dataset`


### Silver layer (cleaned / structured)

- Reads data from the Bronze layer.
- Currently keeps a minimal structure (`day`, `ingestion_ts`, `raw_json`).
- Designed to be extended later with JSON parsing and normalization.

Tables:
- `data/silver/public_power`
- `data/silver/price`


### Gold layer (analytics-ready)

- Produces a final output validating the end-to-end pipeline.
- Currently implemented as a metadata table (`gold/meta`).
- Intended to host daily aggregates and analytical results in future iterations.

Tables:
- `data/gold/meta`

All tables are stored as Delta Lake tables, identified by the presence of a `_delta_log/` directory.

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
- fetch daily energy production and price data from the API,
- store raw data in Bronze Delta tables,
- create Silver tables,
- generate a Gold output validating the end-to-end flow.

(The date range and parameters can be adjusted directly in `main.py`.)

## Idempotency and safe reruns
The pipeline is designed to be safe to rerun.

Before inserting data for a given day:
- existing records for that day are deleted from the Bronze Delta table,
- fresh data is then inserted.

This guarantees:

- no duplicate records,
- reliable backfilling,
- reproducible results.

This behavior is essential for real-world data pipelines.

## Validation
After running the pipeline, Delta Lake transaction logs are created:
```text
data/bronze/public_power/_delta_log
data/bronze/price/_delta_log
data/silver/public_power/_delta_log
data/silver/price/_delta_log
data/gold/meta/_delta_log
```
The presence of _delta_log confirms that the data is stored as Delta Lake tables.

## Design decisions
- Delta Lake was chosen for reliable storage, versioning, and safe overwrites.
- Bronze stores raw JSON to allow reprocessing if schemas change.
- Silver and Gold are intentionally simple placeholders to validate the pipeline structure before adding complex analytics.

## Assumptions and limitations

- The pipeline runs locally using Spark in `local[*]` mode.
- Silver transformations currently keep raw JSON.
- Gold output is a validation artifact rather than a full analytical dataset.