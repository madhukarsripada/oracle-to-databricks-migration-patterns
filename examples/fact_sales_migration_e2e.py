"""
End-to-End Migration Example: Oracle FACT_SALES → Delta Lake silver.fact_sales
================================================================================

This is a complete, production-grade example showing how the patterns in this
repo compose into a real migration. Run as a Databricks notebook, deploy as a
Workflow task, or invoke from a DLT pipeline.

Assumed source schema (Oracle):
    DW.FACT_SALES (
        sale_id        NUMBER(18)    PRIMARY KEY,
        customer_id    NUMBER(18)    NOT NULL,
        product_id     NUMBER(18)    NOT NULL,
        sale_date      DATE          NOT NULL,
        amount         NUMBER(15,2),
        discount       NUMBER(15,2),
        tax            NUMBER(15,2),
        region_code    VARCHAR2(10),
        status         VARCHAR2(1),
        created_at     TIMESTAMP,
        updated_at     TIMESTAMP
    )
    -- Range-partitioned by sale_date, monthly partitions

Migration approach:
    1. Initial load via JDBC (bulk extract from Oracle)
    2. Bronze landing in Delta (1:1 with Oracle source)
    3. Silver with transforms + DQ checks (uses Pattern 12 DECODE→CASE WHEN)
    4. Incremental MERGE for subsequent loads (uses Pattern 1)
    5. Reconciliation (uses examples/reconciliation_framework.py)

Production deployment:
    - Wrap each step as a separate Workflow task for restartability
    - Use Databricks secrets for Oracle credentials
    - Schedule the incremental load on Autosys-equivalent (Workflow trigger)
"""

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col, when, current_timestamp, lit, to_date, year, month,
    sum as _sum, count, broadcast,
)
from pyspark.sql.types import StructType, StructField, StringType, LongType, DecimalType, TimestampType
from delta.tables import DeltaTable


# =============================================================================
# Step 1: Initial extract from Oracle (bulk JDBC read)
# =============================================================================

def initial_extract_from_oracle(
    spark: SparkSession,
    oracle_jdbc_url: str,
    oracle_user: str,
    oracle_password: str,
    partition_date: str,
) -> DataFrame:
    """
    Initial bulk extract of one Oracle partition.

    Production tips:
    - ALWAYS use partitionColumn / lowerBound / upperBound / numPartitions
      to parallelize the read. A single-threaded JDBC read of a 100M row
      Oracle table will run for hours.
    - The partitionColumn should be indexed in Oracle (typically the PK).
    - Set fetchsize to 10000+ to reduce JDBC round-trips.
    """
    oracle_query = f"""
        (SELECT sale_id, customer_id, product_id, sale_date, amount, discount,
                tax, region_code, status, created_at, updated_at
         FROM DW.FACT_SALES
         WHERE sale_date = DATE '{partition_date}') src
    """

    return (
        spark.read.format("jdbc")
        .option("url", oracle_jdbc_url)
        .option("user", oracle_user)
        .option("password", oracle_password)
        .option("driver", "oracle.jdbc.OracleDriver")
        .option("dbtable", oracle_query)
        .option("partitionColumn", "sale_id")
        .option("lowerBound", "1")
        .option("upperBound", "1000000000")
        .option("numPartitions", "16")
        .option("fetchsize", "10000")
        .load()
    )


# =============================================================================
# Step 2: Land to Bronze (1:1 with Oracle, schema-preserved)
# =============================================================================

def write_to_bronze(df: DataFrame, partition_date: str) -> None:
    """
    Bronze table mirrors Oracle structure exactly. No transforms.
    This gives you a replayable source — critical for debugging silver issues.

    Uses replaceWhere for atomic partition swap (Pattern 3).
    """
    (df.withColumn("_bronze_ingested_at", current_timestamp())
       .withColumn("_source_system", lit("oracle.dw.fact_sales"))
       .write.format("delta")
       .mode("overwrite")
       .option("replaceWhere", f"sale_date = '{partition_date}'")
       .saveAsTable("catalog.bronze.fact_sales_raw"))


# =============================================================================
# Step 3: Bronze → Silver with transforms and DQ
# =============================================================================

def transform_bronze_to_silver(spark: SparkSession, partition_date: str) -> DataFrame:
    """
    Apply business transforms to bronze → silver.

    Transforms applied:
    - Map status codes (Pattern 12: DECODE → CASE WHEN)
    - Compute net_amount = amount - discount + tax
    - Lookup region_name from region_code (broadcast join)
    - Filter out invalid rows (status NULL or amount < 0)

    DQ checks:
    - Drop rows with NULL sale_id (corrupt records)
    - Drop rows with negative amount (data quality)
    """
    bronze = (spark.table("catalog.bronze.fact_sales_raw")
              .filter(col("sale_date") == partition_date))

    region_dim = spark.table("catalog.silver.dim_region")  # small dimension

    silver = (bronze
        # DQ: drop corrupt records
        .filter(col("sale_id").isNotNull())
        .filter(col("amount") >= 0)

        # Status decode (Pattern 12)
        .withColumn("status_desc",
            when(col("status") == "A", "Active")
            .when(col("status") == "C", "Cancelled")
            .when(col("status") == "P", "Pending")
            .when(col("status") == "R", "Refunded")
            .otherwise("Unknown"))

        # Computed columns
        .withColumn("net_amount", col("amount") - col("discount") + col("tax"))

        # Region lookup via broadcast join (region_dim is small)
        .join(broadcast(region_dim), on="region_code", how="left")

        # Lineage / audit columns
        .withColumn("_silver_processed_at", current_timestamp())
    )

    return silver.select(
        "sale_id", "customer_id", "product_id", "sale_date",
        "amount", "discount", "tax", "net_amount",
        "region_code", "region_name", "status", "status_desc",
        "created_at", "updated_at",
        "_silver_processed_at",
    )


# =============================================================================
# Step 4: MERGE silver into target (Pattern 1)
# =============================================================================

def merge_into_silver_target(silver_df: DataFrame, partition_date: str) -> dict:
    """
    Idempotent MERGE — re-running the same partition produces the same result.

    Returns the Delta operation metrics for reconciliation logging.
    """
    target = DeltaTable.forName(spark, "catalog.silver.fact_sales")

    (target.alias("t")
        .merge(
            silver_df.alias("s"),
            # Include partition column in merge condition for partition pruning
            "t.sale_id = s.sale_id AND t.sale_date = s.sale_date"
        )
        .whenMatchedUpdate(
            # Only update when source is newer — watermark protection
            condition="t.updated_at < s.updated_at",
            set={
                "amount": "s.amount",
                "discount": "s.discount",
                "tax": "s.tax",
                "net_amount": "s.net_amount",
                "status": "s.status",
                "status_desc": "s.status_desc",
                "updated_at": "s.updated_at",
                "_silver_processed_at": "s._silver_processed_at",
            },
        )
        .whenNotMatchedInsertAll()
        .execute())

    # Capture metrics for reconciliation
    last_op = target.history(1).select("operationMetrics").collect()[0][0]
    return dict(last_op)


# =============================================================================
# Step 5: Reconciliation
# =============================================================================

def run_reconciliation(spark, oracle_jdbc_url: str, oracle_user: str, partition_date: str):
    """
    Verify silver matches Oracle source. Logs to audit table.

    Run this as a separate Workflow task with onFailure → alert.
    """
    from reconciliation_framework import ReconciliationRunner, log_to_audit_table

    runner = ReconciliationRunner(
        oracle_jdbc_url=oracle_jdbc_url,
        oracle_user=oracle_user,
        spark=spark,
        oracle_password_secret_scope="oracle-prod",
        oracle_password_secret_key="dw_reader_password",
    )

    import time
    start = time.time()
    result = runner.reconcile(
        oracle_table="DW.FACT_SALES",
        delta_table="catalog.silver.fact_sales",
        business_keys=["sale_id"],
        numeric_cols=["amount", "discount", "tax"],
        partition_predicate=f"sale_date = DATE '{partition_date}'",
    )
    runtime = time.time() - start

    log_to_audit_table(
        spark, result,
        batch_id=f"fact_sales_{partition_date}",
        run_id=f"recon_{int(time.time())}",
        runtime_seconds=runtime,
    )

    print(result.summary())

    if not result.passed:
        # In production: send to PagerDuty / Slack / email
        raise Exception(
            f"Reconciliation FAILED for fact_sales {partition_date}. "
            f"Reasons: {result.failure_reasons}"
        )


# =============================================================================
# Orchestration — wire as a Databricks Workflow with these tasks
# =============================================================================
"""
Workflow DAG:

    extract_oracle → land_bronze → transform_silver → merge_target → reconcile
                                         ↓
                                       on_failure → alert

Each function above maps to one Workflow task. Pass partition_date as a
Workflow parameter so the same pipeline handles backfills and daily loads.

For incremental loads (after initial), replace `initial_extract_from_oracle`
with a CDC source (Pattern 10) — typically Autoloader reading GoldenGate or
Debezium output landed to ADLS.
"""


if __name__ == "__main__":
    # Local development / testing harness
    spark = SparkSession.builder.appName("fact_sales_migration").getOrCreate()

    # In production these come from Workflow parameters + Databricks secrets
    PARTITION_DATE = "2026-05-31"
    ORACLE_JDBC_URL = "jdbc:oracle:thin:@//oracle-prod.internal:1521/DW"
    ORACLE_USER = "dw_reader"
    # Password retrieved from Databricks secret scope, not hardcoded
    ORACLE_PASSWORD = dbutils.secrets.get("oracle-prod", "dw_reader_password")  # noqa: F821

    # Run the pipeline
    extracted = initial_extract_from_oracle(
        spark, ORACLE_JDBC_URL, ORACLE_USER, ORACLE_PASSWORD, PARTITION_DATE
    )
    write_to_bronze(extracted, PARTITION_DATE)

    silver = transform_bronze_to_silver(spark, PARTITION_DATE)
    metrics = merge_into_silver_target(silver, PARTITION_DATE)
    print(f"MERGE complete: {metrics}")

    run_reconciliation(spark, ORACLE_JDBC_URL, ORACLE_USER, PARTITION_DATE)
