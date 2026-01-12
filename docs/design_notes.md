# Design Decisions - Data Engineering Test
## 1. Tech Stack Justification
**Chosen stack**: Python (Pandas) + DuckDB.

**Justification**: 
Given the data volume and the requirement for local execution, Pandas offers the best balance for complex JSON normalization and metadata merging. DuckDB was chosen as the analytical engine to perform SQL-based "Sanity Checks" on the final curated layer. This stack is significantly lighter than PySpark for this use case, while still being highly performant for medium-sized datasets.

## 2. Modeling & Partitioning
**Data model**
Data model:
A star schema was implemented with:

* fact_order (item-level grain)

* dim_user

* dim_product

This design reduces redundancy and supports efficient analytical queries.

**Fact table granularity**:
Each row represents a unique **(order_id, sku)** combination, allowing orders to contain multiple items.

**Partitioning strategy**:
The curated layer is partitioned by date:
```
output/curated/dt=YYYY-MM-DD/
```

where **dt** is derived from **created_at** from orders.

This Hive-style partitioning is compatible with AWS Redshift (via COPY) and enables partition pruning to reduce I/O and query costs.

**Data types**:

* DECIMAL is used for monetary fields to prevent floating-point precision issues.

* TIMESTAMP (UTC) ensures temporal consistency across systems.

## 3. Idempotency & Incremental Processing
**Idempotency:**
The pipeline is idempotent. Running the job multiple tmes for the same data range does not produce duplicated records.

**Strategy:**
Deduplication is enforced at the `(order_id, sku)` level.

A **read–merge–write** pattern is applied:

1. Existing partition data is read (if present).
2. New records are merged.
3. Duplicates are dropped before writing.

**Incremental loads:**  
The job supports incremental execution via a `--since` parameter, which filters records by `created_at`. This enables efficient reprocessing of only new or updated data without rewriting historical partitions.

## 4. Data Quality, Robustness & Error Handling

**Duplicate records:**  
The API may return duplicate orders. These are handled through explicit deduplication logic.

**Malformed records handling:**
- Missing `order_id` or `created_at`: records are dropped, as they cannot be reliably identified or partitioned.
- Empty `items` array: records are discarded since they have no analytical value.
- Missing `items.price`: defaulted to `0.0` and documented.

**API resilience:**  
The API client includes retry logic with exponential backoff (up to 3 attempts) to handle transient failures.

**Security:**  
No credentials are stored in the repository. Sensitive values (e.g., API keys or database credentials) are expected to be passed via environment variables or a `.env` file excluded from version control.

---

## 5. Observability, Monitoring & Alerting

In a production setup (e.g., Airflow):

**Logging:**  
- Structured logging using Python’s `logging` module.
- Logs suitable for ingestion into CloudWatch or ELK.

**Core metrics:**  
- `records_processed_count`
- `records_dropped_count`
- `execution_time_seconds`

**Alerts:**  
- API failures after exhausting all retries.
- Data quality check failures (e.g., revenue mismatches).
- Execution time exceeding 200% of the historical average.

---

## 6. Redshift Ingestion Strategy

Curated outputs are stored in a Redshift-compatible layout and can be ingested using 'COPY' command:

```
COPY fact_order
FROM 's3://bucket/output/curated/'
IAM_ROLE 'arn:aws:iam::0123456789:role/RedshiftCopyRole'
FORMAT AS CSV 
DELIMITER ',' 
IGNOREHEADER 1;
```