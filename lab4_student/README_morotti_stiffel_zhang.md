# Team: Morotti, Stiffel & Zhang

**DAG id:** `team_morotti_stiffel_zhang`  
**Git repo:** `https://github.com/MaximeMorotti/Big_Data_Processing_Lab_4.git`
**Spark module:** `include/team_morotti_stiffel_zhang_spark.py`  
**Course:** Big Data Processing - Lab 4 Capstone

---

## 1. Business problem

A retail partner provides a daily CSV file containing store transactions. The Operations team needs a daily KPI dashboard to monitor total revenue and transaction counts by category and country. 

If this pipeline fails, the operations team will not have access to the latest KPI metrics, delaying business decisions and performance tracking for that day.

---

## 2. Architecture

<!-- Diagram: incoming → raw/dt= → curated/dt= → reports -->

| Layer | Path | Tool |
|-------|------|------|
| Bronze | `data/incoming/` | `vendor_drop.py` |
| Silver | `data/raw/dt=` | DuckDB (`ingest_day`) |
| Gold | `data/curated/dt=` | `team_morotti_stiffel_zhang_spark.py` |
| Serve | `data/reports/` | JSON dashboard |

### Airflow (5 tasks)

| task_id | Role |
|---------|------|
| `wait_for_csv` | Wait for the vendor CSV file to be dropped (FileSensor) |
| `ingest` | Ingest raw CSV data into Silver Parquet format |
| `validate` | Validate Silver data to catch corruption early (fails visibly if bad data) |
| `compute_kpis` | Run Spark job to compute KPIs (Silver → Gold Parquet & JSON dashboard) |
| `publish_report`| Confirm the JSON dashboard report exists and log its path |

**Dependency graph:**

```
wait_csv → ingest_task → validate_task → compute_task → publish_task
```

---

## 3. Spark transformations (≥3 - your code)

File: `include/team_spark_morotti_stiffel_zhang.py`

| # | Function | What it does |
|---|----------|--------------|
| 1 | `transform_1` | Reads Silver Parquet data and filters out invalid or corrupt rows (e.g., `amount_eur` is null or <= 0). |
| 2 | `transform_2` | Enriches the data by adding the `logical_date` and a derived `revenue_bucket` column ('low', 'medium', 'high' based on transaction amount). |
| 3 | `transform_3` | Aggregates KPIs by grouping by `category` and `country` to compute the `total_revenue_eur` and `transaction_count`. |

---

## 4. Idempotence

When re-running the same logical date (`ds`), the pipeline ensures that no data is duplicated by overwriting existing outputs:
- **`raw/dt=<ds>/`**: DuckDB ingestion process replaces the partition for that day.
- **`curated/dt=<ds>/`**: Spark writes the Gold Parquet using `.mode("overwrite")`.
- **`dashboard_<ds>.json`**: The Spark job completely overwrites the JSON report file for the given date.

---

## 5. Backfill

```bash
docker compose exec airflow-scheduler \
  airflow dags backfill team_morotti_stiffel_zhang -s 2026-06-01 -e 2026-06-07 --reset-dagruns
```

---

## 6. Failure demo

```bash
python scripts/vendor_drop.py --date 2026-06-03 --corrupt
```

When this command is run, the vendor simulator generates corrupt data (e.g., all `amount_eur` = 0). 
In Airflow, the `validate` task will fail visibly (turn red) because it catches the bad data before processing it with Spark. As a result, the downstream tasks (`compute_kpis` and `publish_report`) are skipped.

---

## 7. Exploration tracks

| Track | Done? | Describe your implementation |
|-------|-------|----------|
| R Reliability | | |
| S Spark depth | | |
| O Orchestration | | |
| Q Data quality | | |
| P Custom | | |
| X SparkSubmit | | |

---

## 8. Demo script & backup

---

## 9. Production next steps
