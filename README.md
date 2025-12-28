# Mini Data Engineering Project — Orders Revenue Pipeline

## 1. Goal
Build a mini batch pipeline to compute **daily revenue** from raw CSV files:
- Standardize & clean input
- Validate data quality (reject invalid records)
- Compute daily revenue for BI
- Produce a **quality_report.json** for monitoring

---
## 2. Pipeline Workflow

The pipeline is designed as a **simple batch ETL workflow**, processing data for a single `run_date` per execution.  
It follows clear stage boundaries to ensure data quality, correctness, and safe re-runs.

The workflow separates **staging**, **business transformation**, and **monitoring** concerns, similar to a production batch data pipeline.

### 2.1 High-level Flow

At a high level, the pipeline follows this sequence:

Raw CSV Inputs  
→ Staging & Data Quality Validation  
→ Relationship Validation  
→ Business Transformation  
→ Outputs & Monitoring

---

### 2.2 Workflow Description

1. **Raw Input**  
   The pipeline reads daily CSV files for orders and order items based on the provided `run_date`.

2. **Staging & Data Quality Validation**  
   - Datetime fields are parsed and standardized  
   - Required fields are validated for null values  
   - Orders are deduplicated by `order_id`, keeping the record with the latest `ingested_at`  
   - Invalid records are rejected early to prevent downstream issues  

3. **Relationship Validation**  
   - Order items are validated against the set of valid orders  
   - Items whose `order_id` does not exist in valid orders are treated as orphan items and rejected  

4. **Business Transformation**  
   - Only orders with `status = 'completed'` (case-insensitive) are considered  
   - Revenue is calculated at the line-item level (`quantity * unit_price`)  
   - Results are aggregated by `order_date` to produce a daily revenue mart  

5. **Outputs & Monitoring**  
   - A BI-ready daily revenue dataset is generated  
   - Rejected records are written to separate files for auditability  
   - A quality report is produced to expose data quality and business metrics  

---

## 3. Input Data

The pipeline consumes daily CSV input files located under the `/data/` directory.  
Each pipeline run processes data for **a single `run_date`**, which is passed as a runtime parameter.

### 3.1 Input Files

For a given `run_date`, the expected input files are:

- `orders_<run_date>.csv`
- `order_items_<run_date>.csv`

Example:
```
orders_2024-01-01.csv
order_items_2024-01-01.csv
```

### 3.2 Orders Data

The orders file represents high-level order information.  
Key fields used by the pipeline include:

- `order_id`
- `customer_id`
- `order_date`
- `status`
- `ingested_at`

The orders data may contain:
- Duplicate records for the same `order_id`
- Missing required fields
- Multiple versions of the same order ingested at different times

### 3.3 Order Items Data

The order items file represents line-level order details.  
Key fields used by the pipeline include:

- `order_id`
- `product_id`
- `quantity`
- `unit_price`
- `ingested_at`

The order items data may contain:
- Null quantities
- Invalid or negative prices
- Orphan records whose `order_id` does not exist in the orders dataset

### 3.4 Data Quality Considerations

The input data is treated as **immutable raw data**.  
Records that violate data quality rules are rejected rather than corrected, and are written to separate output files for auditability and debugging purposes.

---

## 4. Processing Requirements

This section describes the processing logic applied by the pipeline, from data staging and validation to business transformation.

### 4.1 Staging (Clean & Standardize)

#### Orders

During the staging phase, the orders dataset is cleaned and standardized as follows:

- Datetime fields (`order_date`, `ingested_at`) are parsed into proper datetime types
- Required fields are validated for null values
- Order status values are standardized for consistent comparison
- Orders are **deduplicated by `order_id`**, keeping the record with the latest `ingested_at`

This ensures that downstream processing operates on the most recent and valid version of each order.

#### Order Items

For order items, the staging process includes:

- Parsing datetime fields such as `ingested_at`
- Casting numeric fields to appropriate types
- Preparing the dataset for data quality validation

---

### 4.2 Data Quality Rules

Data quality checks are applied after staging to prevent invalid records from propagating downstream.

#### Orders

An order record is rejected if any of the following fields is null:

- `order_id`
- `customer_id`
- `order_date`
- `status`

Rejected orders are written to `rejected_orders.csv`.

#### Order Items

An order item record is rejected if:

- `quantity` is null
- `unit_price` is null or less than or equal to 0

Items that pass these checks but whose `order_id` does not exist in the set of valid orders are treated as **orphan items** and are also rejected.

Rejected items are written to `rejected_items.csv`.

The implemented data quality rules mirror the reference logic provided in `sql/checks/dq_checks.sql`.

---

### 4.3 Business Logic

After data quality validation, business logic is applied to compute daily revenue:

- Only orders with `status = 'completed'` (case-insensitive) are considered
- Line item revenue is calculated as:
  ```
  amount = quantity * unit_price
  ```
- Revenue is aggregated at a daily level to produce:
  - `order_date`
  - `total_revenue`
  - `orders_count`

The resulting dataset represents a BI-ready daily revenue mart.



---
## 5. Outputs

All pipeline outputs are written to the `/output/` directory.  
Each run produces a **deterministic snapshot** for the given `run_date`.

### 5.1 Daily Revenue Output

**File:** `daily_revenue.csv`

This file contains the BI-ready daily revenue aggregation with the following columns:

- `order_date` — date of the orders
- `total_revenue` — total revenue for the day
- `orders_count` — number of distinct orders contributing to revenue

This dataset is intended for downstream analytics and reporting use cases.

---

### 5.2 Rejected Records

To support auditability and debugging, records that fail data quality checks are written to separate files:

- **`rejected_orders.csv`**  
  Contains order records rejected due to missing required fields or validation failures.

- **`rejected_items.csv`**  
  Contains order item records rejected due to invalid values or orphan relationships.

These files are generated only if rejected records exist.

---

### 5.3 Quality Report

**File:** `quality_report.json`

The quality report provides structured metrics for monitoring pipeline health and data quality, including:

- Input record counts
- Records rejected during data quality validation
- Records removed due to deduplication
- Orphan item counts
- Business-level metrics such as completed orders and total revenue

The report is designed to make data loss at each stage explicit and easy to reason about.

---

## 6. Idempotency & Safe Re-runs

The pipeline is designed to be **idempotent by default**, ensuring safe re-runs for the same `run_date`.

- Output files are **overwritten** on each execution.
- Running the pipeline multiple times with the same input data and `run_date` produces identical results.
- No state is persisted between runs outside of the generated output files.

This approach avoids duplicate data and simplifies operational behavior, which is appropriate for a batch pipeline processing daily snapshots.

In a production environment, this pattern can be extended to:
- Overwrite partitions by `run_date` in a data warehouse
- Orchestrate runs with retry logic under a workflow scheduler

---

## 7. Assumptions

The following assumptions were made while implementing this pipeline:

- The pipeline processes **one `run_date` per execution**.
- Input data is provided as daily CSV snapshots and is treated as **immutable raw data**.
- Records that violate data quality rules are **rejected rather than corrected**.
- Only orders with `status = 'completed'` (case-insensitive) contribute to revenue.
- Output files represent a **daily snapshot**, not an accumulated historical dataset.
- SQL files under the `/sql` directory are provided as **reference only**; no database engine is used in this implementation.
- The solution prioritizes correctness, clarity, and data quality over heavy optimization or distributed processing.

---

## 8. How to Run

### Prerequisites
- Python 3.10+
- Recommended: virtual environment

### Run the pipeline

```bash
python -m venv .venv
source .venv/bin/activate   # or .\.venv\Scripts\Activate.ps1 on Windows
pip install -r requirements.txt

python etl/run_pipeline.py --run-date 2024-01-01 --input-dir data --output-dir output
```

---

## 9. What’s Next?
If this pipeline were to be extended into a production environment, the following improvements would be considered:
- Run the pipeline under a workflow orchestrator (e.g. Airflow) with scheduling and retries
- Store input and output data in object storage and a data warehouse instead of local files
- Partition outputs by `run_date` for efficient querying
- Add automated data quality alerts based on the metrics in `quality_report.json`
- Introduce basic CI checks for code quality and validation logic
