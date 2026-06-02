"""
Reconciliation Framework for Oracle → Databricks Migrations
=============================================================

Production-grade reconciliation between Oracle source and Delta Lake target.
Adapted from frameworks I built at ADP (audit_batch, audit_step, error_log)
and Credit Acceptance (Oracle→Databricks migration program).

Three reconciliation strategies, in order of strength:

1. Row count match — fastest, catches gross errors only
2. Aggregate hash totals — catches value drift on numeric columns
3. Business key + content hash sampling — catches row-level drift

Run all three for migration validation. Run #1 daily in production.

Usage:
    from reconciliation import ReconciliationRunner

    runner = ReconciliationRunner(
        oracle_jdbc_url="jdbc:oracle:thin:@host:1521/svc",
        oracle_user="reader",
        spark=spark
    )

    result = runner.reconcile(
        oracle_table="DW.FACT_SALES",
        delta_table="catalog.silver.fact_sales",
        business_keys=["sale_id"],
        numeric_cols=["amount", "tax", "discount"],
        partition_predicate="sale_date = DATE '2026-05-31'"
    )

    print(result.summary())
    if not result.passed:
        raise Exception("Reconciliation failed — see audit log")
"""

from dataclasses import dataclass, field
from datetime import datetime
from typing import List, Optional
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    sum as _sum, count, hash as _hash, xxhash64,
    col, lit, concat_ws, current_timestamp,
)


@dataclass
class ReconciliationResult:
    """Result of a reconciliation run. Serializable to audit table."""
    oracle_table: str
    delta_table: str
    run_timestamp: datetime

    # Row counts
    oracle_count: int
    delta_count: int
    count_match: bool = field(init=False)

    # Aggregate hash totals (column → (oracle_sum, delta_sum))
    numeric_aggregates: dict = field(default_factory=dict)
    aggregates_match: bool = field(default=True)

    # Sample row-level mismatches (if any found)
    sample_mismatches: List[dict] = field(default_factory=list)

    # Final verdict
    passed: bool = field(init=False)
    failure_reasons: List[str] = field(default_factory=list)

    def __post_init__(self):
        self.count_match = (self.oracle_count == self.delta_count)
        self.passed = self.count_match and self.aggregates_match and not self.sample_mismatches
        if not self.count_match:
            self.failure_reasons.append(
                f"Row count mismatch: Oracle={self.oracle_count:,} "
                f"Delta={self.delta_count:,} "
                f"diff={self.oracle_count - self.delta_count:+,}"
            )
        if not self.aggregates_match:
            self.failure_reasons.append(
                f"Aggregate mismatch on columns: {list(self.numeric_aggregates.keys())}"
            )
        if self.sample_mismatches:
            self.failure_reasons.append(
                f"{len(self.sample_mismatches)} sample row-level mismatches"
            )

    def summary(self) -> str:
        status = "✓ PASS" if self.passed else "✗ FAIL"
        lines = [
            f"[{status}] {self.oracle_table} → {self.delta_table}",
            f"  Oracle rows: {self.oracle_count:,}",
            f"  Delta rows:  {self.delta_count:,}",
            f"  Diff:        {self.oracle_count - self.delta_count:+,}",
        ]
        if self.numeric_aggregates:
            lines.append("  Aggregates:")
            for col_name, (orc, dlt) in self.numeric_aggregates.items():
                marker = "✓" if orc == dlt else "✗"
                lines.append(f"    {marker} {col_name}: oracle={orc} delta={dlt} diff={orc - dlt:+}")
        if self.failure_reasons:
            lines.append("  Failures:")
            for reason in self.failure_reasons:
                lines.append(f"    - {reason}")
        return "\n".join(lines)


class ReconciliationRunner:
    """Runs reconciliation between an Oracle table and a Delta table."""

    def __init__(
        self,
        oracle_jdbc_url: str,
        oracle_user: str,
        spark: SparkSession,
        oracle_password_secret_scope: Optional[str] = None,
        oracle_password_secret_key: Optional[str] = None,
    ):
        self.oracle_jdbc_url = oracle_jdbc_url
        self.oracle_user = oracle_user
        self.spark = spark
        # In production, fetch password from Databricks secret scope.
        # Never hardcode credentials.
        self._secret_scope = oracle_password_secret_scope
        self._secret_key = oracle_password_secret_key

    def _oracle_password(self) -> str:
        """Fetch Oracle password from Databricks secrets at runtime."""
        if self._secret_scope and self._secret_key:
            return dbutils.secrets.get(  # noqa: F821 — Databricks runtime
                scope=self._secret_scope, key=self._secret_key
            )
        raise RuntimeError(
            "No Oracle password configured. Set oracle_password_secret_scope "
            "and oracle_password_secret_key during construction."
        )

    def _read_oracle_aggregate(
        self,
        oracle_table: str,
        partition_predicate: Optional[str],
        numeric_cols: List[str],
    ) -> dict:
        """
        Push aggregate computation INTO Oracle. Do not pull all rows back.
        Returns {row_count, col_sum_for_each_numeric_col}.
        """
        agg_exprs = ["COUNT(*) AS row_count"]
        for c in numeric_cols:
            # COALESCE protects against NULL semantics differences
            agg_exprs.append(f"NVL(SUM({c}), 0) AS sum_{c}")
        predicate = f"WHERE {partition_predicate}" if partition_predicate else ""
        oracle_query = f"(SELECT {', '.join(agg_exprs)} FROM {oracle_table} {predicate}) agg"

        df = (
            self.spark.read.format("jdbc")
            .option("url", self.oracle_jdbc_url)
            .option("user", self.oracle_user)
            .option("password", self._oracle_password())
            .option("driver", "oracle.jdbc.OracleDriver")
            .option("dbtable", oracle_query)
            .load()
        )
        row = df.collect()[0]
        return {field: row[field] for field in df.columns}

    def _read_delta_aggregate(
        self,
        delta_table: str,
        partition_predicate: Optional[str],
        numeric_cols: List[str],
    ) -> dict:
        """Compute same aggregates on the Delta side."""
        df = self.spark.table(delta_table)
        if partition_predicate:
            df = df.filter(partition_predicate)

        agg_exprs = [count("*").alias("row_count")]
        for c in numeric_cols:
            agg_exprs.append(_sum(col(c)).alias(f"sum_{c}"))

        row = df.agg(*agg_exprs).collect()[0]
        return {field: row[field] for field in row.asDict()}

    def _sample_row_mismatches(
        self,
        oracle_table: str,
        delta_table: str,
        business_keys: List[str],
        content_cols: List[str],
        partition_predicate: Optional[str],
        sample_size: int = 10,
    ) -> List[dict]:
        """
        Compute a deterministic content hash on both sides, find keys whose
        hashes don't match. Returns up to sample_size example mismatches.

        Strategy: concat_ws + xxhash64 produces a deterministic hash per row.
        Same column order, same NULL handling on both sides is critical.
        """
        # Oracle side — push hash down to DB
        cols_oracle = ", ".join(f"COALESCE(TO_CHAR({c}), '~NULL~')" for c in content_cols)
        oracle_hash_query = f"""
            (SELECT {', '.join(business_keys)},
                    ORA_HASH({cols_oracle}) AS row_hash
             FROM {oracle_table}
             {'WHERE ' + partition_predicate if partition_predicate else ''}) h
        """
        oracle_df = (
            self.spark.read.format("jdbc")
            .option("url", self.oracle_jdbc_url)
            .option("user", self.oracle_user)
            .option("password", self._oracle_password())
            .option("driver", "oracle.jdbc.OracleDriver")
            .option("dbtable", oracle_hash_query)
            .load()
            .withColumnRenamed("row_hash", "oracle_hash")
        )

        # Delta side — compute equivalent hash
        delta_df = self.spark.table(delta_table)
        if partition_predicate:
            delta_df = delta_df.filter(partition_predicate)
        delta_df = delta_df.select(
            *business_keys,
            xxhash64(
                concat_ws("|", *[col(c).cast("string") for c in content_cols])
            ).alias("delta_hash"),
        )

        # NOTE: ORA_HASH and xxhash64 produce different hash values. This
        # comparison is illustrative — in real production, either:
        # (a) compute the same hash algorithm on both sides (push xxhash64
        #     down via PL/SQL UDF), or
        # (b) materialize a content-canonical string and hash on Delta side
        #     after JDBC read.
        # The simplified approach below assumes you've harmonized the hash.
        mismatches = (
            oracle_df.join(delta_df, on=business_keys, how="outer")
            .filter(
                (col("oracle_hash").isNull())
                | (col("delta_hash").isNull())
                | (col("oracle_hash") != col("delta_hash"))
            )
            .limit(sample_size)
            .collect()
        )
        return [r.asDict() for r in mismatches]

    def reconcile(
        self,
        oracle_table: str,
        delta_table: str,
        business_keys: List[str],
        numeric_cols: Optional[List[str]] = None,
        partition_predicate: Optional[str] = None,
        run_content_sampling: bool = False,
        content_cols: Optional[List[str]] = None,
    ) -> ReconciliationResult:
        """
        Run the reconciliation suite.

        Args:
            oracle_table:        Fully-qualified Oracle table (SCHEMA.TABLE)
            delta_table:         Fully-qualified Delta table (catalog.schema.table)
            business_keys:       Primary or business key columns
            numeric_cols:        Numeric columns for aggregate hash totals
            partition_predicate: SQL WHERE clause to scope (recommended for large tables)
            run_content_sampling: Enable row-level content hash comparison (expensive)
            content_cols:        Columns to include in content hash (default: all non-key cols)

        Returns:
            ReconciliationResult with verdict and details.
        """
        numeric_cols = numeric_cols or []
        run_ts = datetime.utcnow()

        # Step 1: Aggregate comparison (always run)
        oracle_agg = self._read_oracle_aggregate(oracle_table, partition_predicate, numeric_cols)
        delta_agg = self._read_delta_aggregate(delta_table, partition_predicate, numeric_cols)

        numeric_aggregates = {}
        all_match = True
        for c in numeric_cols:
            o, d = oracle_agg[f"sum_{c}"], delta_agg[f"sum_{c}"]
            numeric_aggregates[c] = (o, d)
            if o != d:
                all_match = False

        # Step 2: Row-level sampling (optional, expensive)
        sample_mismatches = []
        if run_content_sampling and content_cols:
            sample_mismatches = self._sample_row_mismatches(
                oracle_table, delta_table, business_keys,
                content_cols, partition_predicate,
            )

        result = ReconciliationResult(
            oracle_table=oracle_table,
            delta_table=delta_table,
            run_timestamp=run_ts,
            oracle_count=oracle_agg["row_count"],
            delta_count=delta_agg["row_count"],
            numeric_aggregates=numeric_aggregates,
            aggregates_match=all_match,
            sample_mismatches=sample_mismatches,
        )
        return result


# -----------------------------------------------------------------------------
# Audit table DDL — run once to set up the audit log.
# Mirrors the audit_batch / audit_step pattern from ADP / Credit Acceptance.
# -----------------------------------------------------------------------------

AUDIT_TABLE_DDL = """
CREATE TABLE IF NOT EXISTS catalog.audit.reconciliation_log (
    run_id              STRING,
    batch_id            STRING,
    oracle_table        STRING,
    delta_table         STRING,
    partition_predicate STRING,
    run_timestamp       TIMESTAMP,
    oracle_row_count    BIGINT,
    delta_row_count     BIGINT,
    row_count_diff      BIGINT,
    aggregates_json     STRING,
    sample_mismatches_json STRING,
    passed              BOOLEAN,
    failure_reasons     ARRAY<STRING>,
    runtime_seconds     DOUBLE
)
USING DELTA
PARTITIONED BY (DATE(run_timestamp));
"""


def log_to_audit_table(spark, result: ReconciliationResult, batch_id: str, run_id: str, runtime_seconds: float):
    """Append a reconciliation result to the audit log Delta table."""
    import json
    row = [(
        run_id,
        batch_id,
        result.oracle_table,
        result.delta_table,
        None,  # partition_predicate could be stored too
        result.run_timestamp,
        result.oracle_count,
        result.delta_count,
        result.oracle_count - result.delta_count,
        json.dumps({k: list(v) for k, v in result.numeric_aggregates.items()}),
        json.dumps(result.sample_mismatches, default=str),
        result.passed,
        result.failure_reasons,
        runtime_seconds,
    )]
    schema = (
        "run_id STRING, batch_id STRING, oracle_table STRING, delta_table STRING, "
        "partition_predicate STRING, run_timestamp TIMESTAMP, oracle_row_count BIGINT, "
        "delta_row_count BIGINT, row_count_diff BIGINT, aggregates_json STRING, "
        "sample_mismatches_json STRING, passed BOOLEAN, failure_reasons ARRAY<STRING>, "
        "runtime_seconds DOUBLE"
    )
    df = spark.createDataFrame(row, schema)
    df.write.format("delta").mode("append").saveAsTable("catalog.audit.reconciliation_log")
