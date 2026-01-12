# Data Engineering ETL Pipeline

This project implements a robust, idempotent ETL (Extract, Transform, Load) pipeline that fetches order data from a mock API, enriches it with metadata, and stores it in a partitioned storage layer compatible with AWS Redshift and DuckDB.

## Overview
The pipeline follows the **Medallion Architecture** principles:
1.  **Extract:** Fetches incremental data from a JSON API with retry logic.
2.  **Raw Layer:** Saves a point-in-time copy of the original JSON response.
3.  **Transform:** Normalizes nested JSON, validates data quality, and enriches records with User and Product metadata using a Star Schema approach.
4.  **Curated Layer:** Loads data into a partitioned CSV structure (`dt=YYYY-MM-DD`) ensuring **idempotency** via a Read-Merge-Write pattern.

## Tech Stack
* **Language:** Python 3.12+
* **Data Processing:** Pandas
* **Analytical Engine:** DuckDB (for final sanity checks)
* **Testing:** Pytest (with API Mocking)
* **Orchestration:** Makefile

## Project Structure
```
etl-test/
├── data/
│   ├── users.csv
│   └── products.csv
├── docs/
│   └── design_notes.md
├── output/
│   ├── curated/
│   └── raw/
├── sample_data/
│   └── api_orders.json
├── sql/
│   └── redshift-ddl.sql
├── src/
│   ├── __init__.py
│   ├── api_client.py
│   ├── etl_job.py
│   ├── load.py
│   ├── transforms.py
│   └── validations.py
├── tests/
│   ├── __init__.py
│   ├── test_api_client.py
│   ├── test_api_load.py
│   └── test_transforms.py
├── .gitignore
├── makefile
├── README.md
└── requirements.txt
```

## Getting Started
### Prerequisites
* Python 3.12 or higher.
* **make** utility installed.

### Installation & Setup
The project uses a Makefile to automate the environment setup:
```
# 1. Create virtual environment and install dependencies
make install
```
### Running the Pipeline
You can run the full ETL job or specify a date for incremental loading:

```
# Run the full pipeline
make run
```
Note: The **make run** command includes a DuckDB Sanity Check at the end to verify data integrity and business metrics.

### Running Tests
Validated with Pytest, covering transformations, API client resilience, and idempotency:

```
make test
```

### Running everything (Installation, Tests, and ETL)
To install dependencies, run the test suite, and execute the ETL job in one single step, use the following command:
```
make all
```

## Idempotency & Data Integrity

The pipeline is designed to be **idempotent**. This means you can run the job multiple times for the same period without creating duplicate records or corrupting the state.

### How to verify Idempotency:
1.  **Run the ETL multiple times:**
    Execute `make run` or the manual command several times. Check the `output/curated/` folder; the number of records in the CSV files will remain constant because the pipeline deduplicates based on `(order_id, sku)`.
2.  **Manual Deletion Test:**
    * Run the pipeline once to generate results.
    * Manually delete a specific partition folder (e.g., `output/curated/dt=2025-08-20/`) or delete a few rows from a CSV.
    * Run the pipeline again.
    * **Result:** The pipeline will "heal" itself, re-creating the missing data or restoring the deleted records without affecting the rest of the dataset.



## Manual Execution & Parameters

If you are not using the Makefile or want to run specific incremental loads, use the following commands:

### Standard Run
```
python3 -m src.etl_job 
```

### Incremental Load (using --since)
To fetch only records updated after a specific date, use the --since flag:

```
# Format: YYYY-MM-DD
python3 -m src.etl_job --since 2025-08-23
```

### Running the SQL Sanity Checks separately
If you want to run the DuckDB validations without re-running the whole ETL:

```
python3 -c "from src.etl_job import run_sanity_checks; run_sanity_checks()"
```

#### Note:
All commands listed above **must be executed from the root directory** (`etl-test/`). 
If you are inside `src/`, `tests/`, or any other subfolder, the paths for the environment, the modules, and the data outputs will not be resolved correctly.


## SQL Model (Redshift/DuckDB)
The DDL is located in sql/redshift-ddl.sql. It includes:

* Primary and Foreign Keys for relational integrity.

* Optimized data types (TIMESTAMP for transactional dates, DECIMAL for financial values).

* Partitioning strategy based on created_at_order.

## Data Quality & Validation
* API Resilience: Implemented 3 retries with exponential backoff.

* Schema Validation: Filters out records missing critical keys like order_id, sku, or created_at.

* Sanity Checks: Automatic DuckDB execution post-load to confirm the curated layer matches expected totals.

