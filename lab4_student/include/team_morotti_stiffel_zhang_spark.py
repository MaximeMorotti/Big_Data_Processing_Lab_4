"""
include/team_morotti_stiffel_zhang_spark.py
Three Spark transformations: read silver → enrich → aggregate → write Gold + JSON.
"""

from __future__ import annotations

import json
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DoubleType,
    StringType,
    StructField,
    StructType,
)

# ── Paths ──────────────────────────────────────────────────────────────────────
BASE = Path(__file__).resolve().parents[1]


def _silver_path(logical_date: str) -> str:
    return str(BASE / "data" / "raw" / f"dt={logical_date}")


def _curated_path(logical_date: str) -> str:
    return str(BASE / "data" / "curated" / f"dt={logical_date}")


def _report_path(logical_date: str) -> Path:
    return BASE / "data" / "reports" / f"dashboard_{logical_date}.json"


# ── Schema ─────────────────────────────────────────────────────────────────────
SILVER_SCHEMA = StructType(
    [
        StructField("tx_id", StringType(), True),
        StructField("category", StringType(), True),
        StructField("country", StringType(), True),
        StructField("amount_eur", DoubleType(), True),
    ]
)


# ── Transformations ────────────────────────────────────────────────────────────


def transform_1(spark: SparkSession, logical_date: str) -> DataFrame:
    """Read Silver Parquet with explicit schema and filter out invalid rows.

    Drops rows where amount_eur is null or <= 0 (corrupt vendor data).
    """
    df = spark.read.schema(SILVER_SCHEMA).parquet(_silver_path(logical_date))
    df = df.filter(F.col("amount_eur").isNotNull() & (F.col("amount_eur") > 0))
    return df


def transform_2(spark: SparkSession, df: DataFrame, logical_date: str) -> DataFrame:
    """Enrich: add logical_date column and a revenue_bucket derived column.

    revenue_bucket: 'low' (<50), 'medium' (50–200), 'high' (>200).
    """
    df = df.withColumn("logical_date", F.lit(logical_date))
    df = df.withColumn(
        "revenue_bucket",
        F.when(F.col("amount_eur") < 50, "low")
        .when(F.col("amount_eur") <= 200, "medium")
        .otherwise("high"),
    )
    return df


def transform_3(df: DataFrame) -> DataFrame:
    """Aggregate KPIs by category and country.

    Produces: category, country, total_revenue_eur, transaction_count.
    """
    return (
        df.groupBy("category", "country")
        .agg(
            F.round(F.sum("amount_eur"), 2).alias("total_revenue_eur"),
            F.count("tx_id").alias("transaction_count"),
        )
        .orderBy("category", "country")
    )


# ── Entry point ────────────────────────────────────────────────────────────────


def run_daily(logical_date: str, *, with_reference: bool = False) -> dict:
    """Called from the Airflow @task. Idempotent: overwrites existing outputs."""
    spark = (
        SparkSession.builder.appName(f"team_morotti_stiffel_zhang_{logical_date}")
        .master("local[*]")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    try:
        # Pipeline
        df1 = transform_1(spark, logical_date)
        df2 = transform_2(spark, df1, logical_date)
        df3 = transform_3(df2)

        # Write Gold Parquet (overwrite for idempotence)
        curated = _curated_path(logical_date)
        df3.write.mode("overwrite").parquet(curated)

        # Collect totals for the JSON report
        totals = df3.agg(
            F.round(F.sum("total_revenue_eur"), 2).alias("total_revenue_eur"),
            F.sum("transaction_count").alias("transaction_count"),
        ).collect()[0]

        report = {
            "logical_date": logical_date,
            "total_revenue_eur": float(totals["total_revenue_eur"] or 0),
            "transaction_count": int(totals["transaction_count"] or 0),
            "curated_path": curated,
            "status": "ok",
        }

        # Write JSON report (overwrite for idempotence)
        report_path = _report_path(logical_date)
        report_path.parent.mkdir(parents=True, exist_ok=True)
        report_path.write_text(json.dumps(report, indent=2), encoding="utf-8")

        print(f"[run_daily] Done for ds={logical_date}: {report}")
        return report

    finally:
        spark.stop()
