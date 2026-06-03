# Oracle to Databricks Migration Patterns

> Production-grade translation patterns for migrating Oracle Data Warehouse workloads (PL/SQL, ODI, Exadata) to the Databricks Lakehouse (Delta Lake, PySpark, Workflows).

**Author:** Madhukar Sripada — 18+ years Oracle Data Engineering, currently leading Oracle-to-Databricks migration programs for enterprise clients.

**Companion tool:** [oracletospark.io](https://oracletospark.io) — interactive Oracle SQL to Spark SQL converter.

---

## Why this exists

Most Oracle-to-Databricks migration content treats the move as a fresh greenfield build. In the real world, you inherit 15 years of PL/SQL packages, ODI mappings, GoldenGate flows, and Exadata-tuned SQL — and the migration succeeds or fails based on whether the engineering team can faithfully translate the *intent* of the legacy code, not just the syntax.

This repo documents 12 high-frequency translation patterns I've encountered repeatedly across Oracle DW migrations at MasterCard, ADP, Credit Acceptance, and the FDA. Each pattern shows:

- The Oracle/PL/SQL/ODI source pattern
- The Databricks equivalent (Delta Lake, PySpark, or Spark SQL)
- Why the naive translation is wrong
- Production gotchas, performance considerations, and the reconciliation/audit angle

If you're a data engineer staring at a 500-table Oracle warehouse and wondering where to start, this is the reference I wish I had when I started.

---

## The 12 Patterns

| # | Pattern | Source | Target |
|---|---------|--------|--------|
| 01 | [MERGE Statement] | Oracle MERGE | Delta MERGE INTO |
| 02 | [SCD Type 2 Dimension](02-scd-type-2.md) | PL/SQL package with MERGE | Delta MERGE with effective dates |
| 03 | [Partition Exchange Loading](03-partition-exchange.md) | Oracle EXCHANGE PARTITION | Delta REPLACE WHERE |
| 04 | [Materialized Views](04-materialized-views.md) | FAST refresh MV | Delta Live Tables (DLT) |
| 05 | [BULK COLLECT / FORALL](05-bulk-collect.md) | PL/SQL bulk operations | Spark batch read/write |
| 06 | [Cursor Loops](06-cursor-loops.md) | PL/SQL FOR cursor LOOP | DataFrame operations (anti-pattern) |
| 07 | [Hierarchical Queries](07-connect-by.md) | CONNECT BY | Recursive CTE / iterative DataFrame |
| 08 | [Analytic / Window Functions](08-window-functions.md) | Oracle analytic functions | Spark window functions |
| 09 | [ODI Knowledge Modules](09-odi-to-workflows.md) | ODI LKM / IKM / CKM | Databricks Workflows + DLT EXPECT |
| 10 | [Change Data Capture](10-cdc.md) | GoldenGate Extract/Replicat | Autoloader / DLT CDC |
| 11 | [ROWNUM / ROW_NUMBER](11-rownum.md) | Oracle ROWNUM | Spark row_number() with caveats |
| 12 | [DECODE and Conditional Logic](12-decode.md) | DECODE / NVL / NVL2 | CASE WHEN / when().otherwise() |

---

## Pattern 1: Oracle MERGE → Delta MERGE INTO

This is the single most common migration pattern. Roughly 60% of PL/SQL ETL code I've migrated centers on MERGE for upsert logic.

### Oracle source

```sql
MERGE INTO dim_customer t
USING (
    SELECT customer_id, customer_name, email, region, updated_at
    FROM stg_customer
    WHERE batch_id = :batch_id
) s
ON (t.customer_id = s.customer_id)
WHEN MATCHED THEN
    UPDATE SET
        t.customer_name = s.customer_name,
        t.email = s.email,
        t.region = s.region,
        t.updated_at = s.updated_at
    WHERE t.updated_at < s.updated_at      -- watermark filter
WHEN NOT MATCHED THEN
    INSERT (customer_id, customer_name, email, region, updated_at)
    VALUES (s.customer_id, s.customer_name, s.email, s.region, s.updated_at);
COMMIT;
```

### Databricks target (Spark SQL)

```sql
MERGE INTO catalog.silver.dim_customer t
USING (
    SELECT customer_id, customer_name, email, region, updated_at
    FROM catalog.bronze.stg_customer
    WHERE batch_id = :batch_id
) s
ON t.customer_id = s.customer_id
WHEN MATCHED AND t.updated_at < s.updated_at THEN
    UPDATE SET
        customer_name = s.customer_name,
        email = s.email,
        region = s.region,
        updated_at = s.updated_at
WHEN NOT MATCHED THEN
    INSERT (customer_id, customer_name, email, region, updated_at)
    VALUES (s.customer_id, s.customer_name, s.email, s.region, s.updated_at);
```

### Production gotchas

**Watermark predicate position.** In Oracle, the `WHERE` after `UPDATE SET` filters which matched rows get updated. In Delta MERGE, the equivalent goes into the `WHEN MATCHED AND ...` clause, not a separate WHERE. Get this wrong and you'll overwrite newer rows with older data — a silent data quality bug that won't surface until reconciliation.

**File rewrites are expensive.** Delta MERGE rewrites entire parquet files containing matched rows, not just the matched rows themselves. On a 1B-row table with random updates, this is catastrophic. Mitigations:
- Z-ORDER the table on the merge key so matched rows cluster in fewer files
- Use partition pruning: include the partition column in the merge condition if possible (`ON t.customer_id = s.customer_id AND t.load_date = s.load_date`)
- Set `spark.databricks.delta.merge.repartitionBeforeWrite.enabled = true` for large merges

**No COMMIT statement.** Delta is ACID by default. Each MERGE is one transaction. There's no concept of intermediate commits like the PL/SQL pattern of "COMMIT every 1000 rows" — and you don't want one.

**Reconciliation hook.** In ADP I always followed every MERGE with a reconciliation count: source row count vs. target affected row count (insert + update). The Delta operation metrics expose this:

```python
from delta.tables import DeltaTable
deltaTable = DeltaTable.forName(spark, "catalog.silver.dim_customer")
last_op = deltaTable.history(1).select("operationMetrics").collect()[0][0]
# {numTargetRowsInserted: ..., numTargetRowsUpdated: ..., numTargetRowsDeleted: ...}
```

### When the naive translation breaks

If your PL/SQL MERGE uses `WHERE` after both `UPDATE SET` and `INSERT`, and the columns referenced in the WHERE are not in the ON clause, the translation is non-trivial. Spark requires those predicates to be in the MATCHED/NOT MATCHED conditions, which may force a redesign of the staging query.

---

## Pattern 2: SCD Type 2 Dimension

PL/SQL SCD Type 2 packages typically use a two-step process: (1) expire existing current rows where a non-key attribute changed, (2) insert new current rows.

### Oracle source (simplified PL/SQL)

```sql
-- Step 1: Expire current rows where tracked attributes changed
UPDATE dim_customer t
SET end_date = SYSDATE - INTERVAL '1' SECOND,
    is_current = 'N'
WHERE is_current = 'Y'
  AND EXISTS (
      SELECT 1 FROM stg_customer s
      WHERE s.customer_id = t.customer_id
        AND (s.customer_name <> t.customer_name OR
             s.region        <> t.region        OR
             s.segment       <> t.segment)
  );

-- Step 2: Insert new current rows
INSERT INTO dim_customer (
    customer_sk, customer_id, customer_name, region, segment,
    effective_date, end_date, is_current
)
SELECT
    seq_customer_sk.NEXTVAL,
    s.customer_id, s.customer_name, s.region, s.segment,
    SYSDATE, DATE '9999-12-31', 'Y'
FROM stg_customer s
WHERE NOT EXISTS (
    SELECT 1 FROM dim_customer t
    WHERE t.customer_id = s.customer_id
      AND t.is_current = 'Y'
      AND t.customer_name = s.customer_name
      AND t.region = s.region
      AND t.segment = s.segment
);
```

### Databricks target (single MERGE, idiomatic)

```sql
MERGE INTO catalog.silver.dim_customer t
USING (
    -- Source: produce two logical rows per changed customer
    -- one to close the old version, one to open the new
    SELECT
        s.customer_id AS merge_key,
        s.customer_id, s.customer_name, s.region, s.segment,
        current_timestamp() AS effective_date
    FROM catalog.bronze.stg_customer s

    UNION ALL

    SELECT
        NULL AS merge_key,             -- forces NOT MATCHED → insert new version
        s.customer_id, s.customer_name, s.region, s.segment,
        current_timestamp() AS effective_date
    FROM catalog.bronze.stg_customer s
    INNER JOIN catalog.silver.dim_customer t
        ON s.customer_id = t.customer_id
       AND t.is_current = true
    WHERE t.customer_name <> s.customer_name
       OR t.region        <> s.region
       OR t.segment       <> s.segment
) staged
ON t.customer_id = staged.merge_key AND t.is_current = true
WHEN MATCHED AND (
        t.customer_name <> staged.customer_name OR
        t.region        <> staged.region        OR
        t.segment       <> staged.segment
   ) THEN
    UPDATE SET
        is_current = false,
        end_date   = staged.effective_date
WHEN NOT MATCHED THEN
    INSERT (customer_id, customer_name, region, segment,
            effective_date, end_date, is_current)
    VALUES (staged.customer_id, staged.customer_name, staged.region, staged.segment,
            staged.effective_date, TIMESTAMP '9999-12-31 00:00:00', true);
```

### Production gotchas

**No SEQUENCE in Delta.** Oracle's `seq_customer_sk.NEXTVAL` has no direct Delta equivalent. Options:
1. Drop the surrogate key entirely. Use the natural key + effective_date as the row identifier. This is the modern lakehouse approach.
2. Use `IDENTITY` columns (Databricks GENERATED ALWAYS AS IDENTITY) — works but has gaps and isn't reproducible.
3. Compute surrogate as `hash(customer_id, effective_date)` — deterministic, no global counter needed.

**The two-row UNION pattern is the SCD Type 2 idiom in Delta.** Memorize it. The trick is the NULL merge_key in the second branch — it can't match any existing row, so it forces an INSERT. This is the standard pattern documented by Databricks and it's what interviewers expect to see.

**Watermark / late-arriving rows.** If your source has late-arriving rows (a Tuesday update that arrives Thursday), the simple "current_timestamp()" effective_date is wrong. Use the business event timestamp from the source, and add logic to handle out-of-order arrivals — typically by closing the row whose `effective_date` is the largest one less than the new row's timestamp.

### Talking-point for interviews

When asked "how would you do SCD Type 2 in Databricks?" — lead with the two-row UNION MERGE pattern, then mention the surrogate key tradeoff, then close with late-arriving data. That's a 90-second answer that demonstrates you've actually done this in production.

---

## Pattern 3: Partition Exchange → Delta REPLACE WHERE

Oracle's partition exchange is one of the most powerful loading patterns — atomic swap of a fully-loaded staging table into a partition of a target table. The Delta equivalent is `REPLACE WHERE`.

### Oracle source

```sql
-- Load staging table to match partition structure
INSERT /*+ APPEND */ INTO stg_sales_2026_q1
SELECT * FROM raw.sales WHERE sale_date BETWEEN DATE '2026-01-01' AND DATE '2026-03-31';

-- Build indexes on staging (matches target)
ALTER INDEX stg_sales_2026_q1_idx UNUSABLE;
ALTER INDEX stg_sales_2026_q1_idx REBUILD;

-- Atomic partition swap
ALTER TABLE fact_sales
    EXCHANGE PARTITION p_2026_q1 WITH TABLE stg_sales_2026_q1
    INCLUDING INDEXES
    WITHOUT VALIDATION;
```

### Databricks target

```python
from pyspark.sql.functions import col

# Build the new partition data
new_partition_df = (
    spark.table("catalog.bronze.raw_sales")
    .filter((col("sale_date") >= "2026-01-01") & (col("sale_date") <= "2026-03-31"))
    .transform(apply_silver_transforms)
)

# Atomic partition replacement
(new_partition_df.write
    .format("delta")
    .mode("overwrite")
    .option("replaceWhere", "sale_date >= '2026-01-01' AND sale_date <= '2026-03-31'")
    .saveAsTable("catalog.silver.fact_sales"))
```

### Production gotchas

**The replaceWhere predicate must match the partition boundaries exactly.** If your Delta table is partitioned on `sale_date` and you write a predicate that doesn't align (e.g. `sale_date >= '2026-01-01'` without an upper bound), you'll overwrite more data than intended. Always be explicit about both bounds.

**Predicate must reference partition columns only.** Unlike Oracle's exchange, Delta's replaceWhere works with any predicate, but for performance and correctness you should restrict it to partition columns. Otherwise Delta has to scan files to determine what to overwrite, defeating the atomic-swap benefit.

**Schema evolution is allowed.** Unlike Oracle's strict schema match requirement on exchange, Delta with `mergeSchema = true` allows the new partition data to have new columns. Be careful — this can mask source schema drift.

**Statistics collection.** Oracle's partition swap preserves stats on the new partition. In Delta, run `ANALYZE TABLE ... COMPUTE STATISTICS` after a large partition replace, or rely on auto-stats with `delta.autoOptimize.autoCompact = true`.

---

## Pattern 4: Materialized Views → Delta Live Tables (DLT)

Oracle materialized views with FAST refresh are the bread-and-butter of incremental aggregation. The Databricks equivalent is Delta Live Tables (DLT) with streaming sources.

### Oracle source

```sql
CREATE MATERIALIZED VIEW LOG ON fact_sales
WITH ROWID, SEQUENCE (sale_date, region, amount)
INCLUDING NEW VALUES;

CREATE MATERIALIZED VIEW mv_sales_daily
BUILD IMMEDIATE
REFRESH FAST ON DEMAND
ENABLE QUERY REWRITE
AS
SELECT sale_date, region, SUM(amount) AS total_amount, COUNT(*) AS txn_count
FROM fact_sales
GROUP BY sale_date, region;

-- Refresh
BEGIN
    DBMS_MVIEW.REFRESH('mv_sales_daily', 'F');
END;
```

### Databricks target (DLT)

```python
import dlt
from pyspark.sql.functions import sum as _sum, count, col

@dlt.table(
    name="silver_sales_clean",
    comment="Cleaned sales transactions",
    table_properties={"quality": "silver"}
)
@dlt.expect_or_drop("valid_amount", "amount > 0")
@dlt.expect_or_drop("valid_date", "sale_date IS NOT NULL")
def silver_sales_clean():
    return (
        dlt.read_stream("bronze_sales_raw")
        .select("sale_id", "sale_date", "region", "amount")
    )

@dlt.table(
    name="gold_sales_daily",
    comment="Daily sales aggregation by region — replaces Oracle mv_sales_daily"
)
def gold_sales_daily():
    return (
        dlt.read_stream("silver_sales_clean")
        .groupBy("sale_date", "region")
        .agg(
            _sum("amount").alias("total_amount"),
            count("*").alias("txn_count")
        )
    )
```

### Production gotchas

**DLT EXPECT is the ODI CKM equivalent.** In ODI, Check Knowledge Modules ran data quality checks during load. DLT's `@dlt.expect_or_drop`, `@dlt.expect_or_fail`, and `@dlt.expect` decorators serve the same role with better observability. This is a strong talking point — it shows you understand the architectural equivalence, not just the syntactic one.

**Incremental vs. complete refresh.** Oracle's FAST refresh requires an MV log and specific aggregate rules. DLT handles this automatically with streaming sources: use `dlt.read_stream()` for incremental, `dlt.read()` for complete refresh. Match these to the Oracle source's refresh mode.

**Query rewrite has no direct equivalent.** Oracle's `ENABLE QUERY REWRITE` let the optimizer transparently substitute the MV for a query. Databricks doesn't do this. The closest equivalent is materialized views (in Databricks SQL) which can be referenced explicitly. Don't promise interview panels that Databricks does Oracle-style query rewrite — it doesn't, and that's a knowledge tell.

---

## Pattern 5: BULK COLLECT / FORALL → Spark Batch Operations

Oracle's BULK COLLECT with LIMIT and FORALL with SAVE EXCEPTIONS is the high-performance bulk DML pattern. In Spark, the equivalent is implicit — Spark *always* operates in bulk — but the error-handling semantics need a deliberate redesign.

### Oracle source

```sql
DECLARE
    CURSOR c_src IS SELECT customer_id, email FROM stg_customer;
    TYPE t_rec IS TABLE OF c_src%ROWTYPE;
    l_rec t_rec;
    bulk_errors EXCEPTION;
    PRAGMA EXCEPTION_INIT(bulk_errors, -24381);
BEGIN
    OPEN c_src;
    LOOP
        FETCH c_src BULK COLLECT INTO l_rec LIMIT 10000;
        EXIT WHEN l_rec.COUNT = 0;

        BEGIN
            FORALL i IN 1..l_rec.COUNT SAVE EXCEPTIONS
                UPDATE dim_customer
                SET email = l_rec(i).email
                WHERE customer_id = l_rec(i).customer_id;
        EXCEPTION
            WHEN bulk_errors THEN
                FOR j IN 1..SQL%BULK_EXCEPTIONS.COUNT LOOP
                    INSERT INTO etl_error_log (customer_id, error_msg, batch_id)
                    VALUES (
                        l_rec(SQL%BULK_EXCEPTIONS(j).ERROR_INDEX).customer_id,
                        SQLERRM(-SQL%BULK_EXCEPTIONS(j).ERROR_CODE),
                        :batch_id
                    );
                END LOOP;
        END;
    END LOOP;
    CLOSE c_src;
    COMMIT;
END;
```

### Databricks target (PySpark)

```python
from pyspark.sql.functions import col, current_timestamp, lit

# Read source — Spark handles batching automatically
src_df = spark.table("catalog.bronze.stg_customer").select("customer_id", "email")

# Apply the MERGE — Spark's bulk-by-default model means no LIMIT loop
(spark.read.table("catalog.silver.dim_customer").alias("t")
    .merge(src_df.alias("s"), "t.customer_id = s.customer_id")
    .whenMatchedUpdate(set={"email": "s.email"})
    .execute())

# Error handling: validate first, log bad rows, then merge clean rows
bad_rows = src_df.filter(
    col("email").isNull() | (~col("email").rlike(r"^[^@]+@[^@]+\.[^@]+$"))
)
(bad_rows
    .withColumn("error_msg", lit("Invalid email format"))
    .withColumn("batch_id", lit(batch_id))
    .withColumn("logged_at", current_timestamp())
    .write.format("delta").mode("append")
    .saveAsTable("catalog.audit.etl_error_log"))

clean_rows = src_df.subtract(bad_rows.select("customer_id", "email"))
# Then merge clean_rows...
```

### Production gotchas

**Spark's failure model is fail-fast at the task level.** SAVE EXCEPTIONS has no direct equivalent — a Spark job either succeeds entirely or fails. The idiomatic replacement is **validate-first, then load**: filter known-bad rows into an error log before the MERGE, so the MERGE itself never sees them.

**Don't write a Python loop over partitions to mimic FORALL LIMIT.** This is the #1 PL/SQL migration anti-pattern. Spark's whole architecture is built around bulk distributed execution; chunking into 10,000-row batches inside a Python loop runs everything on the driver and defeats the entire engine.

**Use DLT EXPECT for declarative quality.** If this is a DLT pipeline, the bad-email check belongs in `@dlt.expect_or_drop("valid_email", "email RLIKE '^[^@]+@[^@]+\\.[^@]+$'")`. Cleaner and integrates with the data quality dashboard.

---

## Pattern 6: PL/SQL Cursor Loops → DataFrame Operations (Anti-Pattern Lesson)

This is the most common migration *mistake*, not just a pattern. PL/SQL developers new to Spark often translate cursor loops literally, producing code that works but runs 100x slower than it should.

### Oracle source (the row-by-row habit)

```sql
DECLARE
    CURSOR c_orders IS SELECT order_id, customer_id, total FROM stg_orders;
    v_tier VARCHAR2(10);
    v_discount NUMBER;
BEGIN
    FOR r IN c_orders LOOP
        -- Compute tier
        IF r.total > 1000 THEN v_tier := 'GOLD';
        ELSIF r.total > 500 THEN v_tier := 'SILVER';
        ELSE v_tier := 'BRONZE';
        END IF;

        -- Lookup discount from another table
        SELECT discount_pct INTO v_discount
        FROM customer_tiers
        WHERE customer_id = r.customer_id;

        -- Insert enriched record
        INSERT INTO enriched_orders (order_id, tier, final_total)
        VALUES (r.order_id, v_tier, r.total * (1 - v_discount/100));
    END LOOP;
    COMMIT;
END;
```

### Databricks target (the Spark way)

```python
from pyspark.sql.functions import when, col

orders = spark.table("catalog.bronze.stg_orders")
tiers  = spark.table("catalog.silver.customer_tiers")

enriched = (orders.alias("o")
    .join(tiers.alias("t"), "customer_id", "left")
    .withColumn("tier",
        when(col("total") > 1000, "GOLD")
        .when(col("total") > 500, "SILVER")
        .otherwise("BRONZE"))
    .withColumn("final_total", col("total") * (1 - col("discount_pct") / 100))
    .select("order_id", "tier", "final_total"))

enriched.write.format("delta").mode("append").saveAsTable("catalog.silver.enriched_orders")
```

### Why the loop translation fails

**Naive translation:** A junior engineer might write a `for row in orders.collect():` loop in Python and do per-row lookups. This:
1. Pulls all data to the driver (`collect()`) — fails on any non-trivial dataset
2. Performs serial row-by-row processing — defeats Spark entirely
3. Issues one Delta write per row — produces millions of tiny files

**The mental shift:** PL/SQL cursor loops are *imperative* — "for each row, do these steps." Spark is *declarative* — "describe the final shape of the data." Every cursor loop has a set-based equivalent. Finding it is the core skill.

### Interview tell

If a candidate translates a PL/SQL cursor loop literally into a Python loop, they don't understand Spark's execution model. This is one of the questions screeners use to separate "knows Databricks syntax" from "thinks in Spark."

---

## Pattern 7: CONNECT BY → Recursive CTE

Oracle's `CONNECT BY` is concise. Spark requires recursive CTEs (supported in Databricks Runtime 14.3+) or iterative DataFrame joins.

### Oracle source

```sql
SELECT employee_id, manager_id, employee_name, LEVEL AS depth,
       SYS_CONNECT_BY_PATH(employee_name, ' > ') AS hierarchy_path
FROM employees
START WITH manager_id IS NULL
CONNECT BY PRIOR employee_id = manager_id;
```

### Databricks target (Spark SQL recursive CTE, DBR 14.3+)

```sql
WITH RECURSIVE org_tree AS (
    -- Anchor: top of hierarchy
    SELECT employee_id, manager_id, employee_name,
           1 AS depth,
           CAST(employee_name AS STRING) AS hierarchy_path
    FROM employees
    WHERE manager_id IS NULL

    UNION ALL

    -- Recursive step
    SELECT e.employee_id, e.manager_id, e.employee_name,
           t.depth + 1,
           CONCAT(t.hierarchy_path, ' > ', e.employee_name)
    FROM employees e
    JOIN org_tree t ON e.manager_id = t.employee_id
)
SELECT * FROM org_tree;
```

### Production gotchas

**Recursive CTEs require DBR 14.3+.** On older runtimes, you implement it as an iterative DataFrame loop with a termination condition (joining the previous iteration with the source until no new rows are added). Know both approaches — older enterprise Databricks deployments often lag on runtime versions.

**No cycle detection by default.** Oracle has `NOCYCLE`. Spark recursive CTEs will run forever on cyclic data. Add a depth limit guard: `WHERE depth < 100` in the recursive step.

**Performance.** For deep hierarchies (>20 levels), recursive CTEs in Spark are slow. Consider materializing the hierarchy nightly into a denormalized "closure table" instead of computing on every query.

---

## Pattern 8: Analytic / Window Functions

Good news: this is mostly a 1:1 translation. Spark SQL implements the SQL standard window functions, and most Oracle analytic SQL ports cleanly.

### Oracle and Databricks (essentially identical)

```sql
-- Both Oracle and Spark SQL:
SELECT
    customer_id,
    order_date,
    amount,
    SUM(amount) OVER (
        PARTITION BY customer_id
        ORDER BY order_date
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS running_total,
    LAG(amount, 1) OVER (PARTITION BY customer_id ORDER BY order_date) AS prev_amount,
    ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY order_date DESC) AS rn
FROM orders;
```

### Things to watch

**KEEP DENSE_RANK FIRST/LAST.** Oracle has `MIN(x) KEEP (DENSE_RANK FIRST ORDER BY y)` — Spark doesn't. Rewrite with a windowed `FIRST_VALUE`/`LAST_VALUE`.

**LISTAGG vs. CONCAT_WS+COLLECT_LIST.** Oracle's `LISTAGG(name, ',') WITHIN GROUP (ORDER BY name)` becomes Spark's `CONCAT_WS(',', SORT_ARRAY(COLLECT_LIST(name)))`. Looks ugly, works correctly.

**RANGE windows on dates.** Oracle's `RANGE BETWEEN INTERVAL '7' DAY PRECEDING AND CURRENT ROW` requires Spark to have the ordering column as a numeric type (Unix timestamp) or you'll hit type errors. Cast to long before the window.

---

## Pattern 9: ODI Knowledge Modules → Databricks Workflows + DLT

This one isn't a code translation — it's an architectural mapping. ODI's three-tier topology (Topology, Designer, Operator) and Knowledge Module abstraction map cleanly onto Databricks primitives once you understand the equivalence.

### ODI concept → Databricks concept

| ODI Construct | Databricks Equivalent |
|---|---|
| Load Plan | Databricks Workflow (DAG of tasks) |
| Interface / Mapping | DLT pipeline or notebook task |
| LKM (Loading KM) — source extraction | Autoloader / `spark.read.format(...)` |
| IKM (Integration KM) — target write | Delta MERGE / DLT @dlt.table |
| CKM (Check KM) — data quality | DLT `@dlt.expect_*` decorators |
| JKM (Journalizing KM) — CDC | Autoloader with file-notification mode, or DLT CDC |
| Smart Export / Smart Import | Databricks Asset Bundles (DABs) for env promotion |
| Operator (run monitoring) | Workflow run history + system tables |
| Topology (env config) | Workspace + Unity Catalog metastore |
| Scenario | Compiled workflow JSON in version control |

### Production gotchas

**ODI's restart/recovery from a failed step has no clean equivalent.** ODI lets you restart a load plan from the failed step. Databricks Workflows restart from the failed task — similar but the unit of granularity differs. For complex multi-step ODI mappings, you may need to decompose into smaller Workflow tasks to match the restart granularity.

**Custom Knowledge Modules don't translate.** If your shop has hand-crafted IKMs (e.g., a custom IKM Oracle Incremental Update with audit and partition exchange), the *intent* maps to DLT or MERGE patterns, but the implementation is rebuilt from scratch. Plan for this in migration effort estimates.

**Smart Export/Import → Databricks Asset Bundles.** DABs are the modern Databricks deployment unit (YAML-defined, source-controlled, env-aware). They're the cleanest analog to ODI's Smart Export/Import lifecycle. Learn DABs — this is a 2025/2026 interview hot topic.

---

## Pattern 10: GoldenGate CDC → Autoloader / DLT CDC

GoldenGate's Extract/RPUMP/Replicat chain is replaced in Databricks by a combination of CDC source (typically Kafka, Debezium, or Oracle XStream output) plus Autoloader or DLT's `APPLY CHANGES INTO`.

### Architectural pattern

```
Oracle Source DB
    │
    ├─── Option A: Oracle GoldenGate for Big Data → Kafka topic → Autoloader/DLT
    ├─── Option B: Debezium Oracle connector → Kafka topic → Autoloader/DLT
    └─── Option C: Oracle XStream → file landing → Autoloader → DLT APPLY CHANGES
```

### DLT APPLY CHANGES (the CDC sink)

```python
import dlt

dlt.create_streaming_table("silver_customer")

dlt.apply_changes(
    target = "silver_customer",
    source = "bronze_customer_cdc",
    keys = ["customer_id"],
    sequence_by = "_change_timestamp",   # the GG/Debezium event timestamp
    apply_as_deletes = "_change_op = 'DELETE'",
    except_column_list = ["_change_op", "_change_timestamp"],
    stored_as_scd_type = 1   # or 2 for history retention
)
```

### Production gotchas

**Watermarks and out-of-order events.** GoldenGate guarantees ordering within a single source table via the RPUMP trail file. Kafka-based delivery does not — partition-level ordering only. If you key the Kafka topic by primary key, you get per-key ordering, which is usually what you need.

**Initial snapshot vs. incremental.** GoldenGate handles initial load + ongoing CDC in one tool. In Databricks you typically split these: an initial Spark JDBC read for the snapshot, then switch to the CDC stream for ongoing. The cutover window needs careful handling — a brief overlap is normal.

**Schema evolution.** GoldenGate handles DDL replication. Autoloader handles schema inference and evolution. DLT handles schema enforcement with `pipelines.reset.allowed`. Each of these has different semantics — pick deliberately and document the choice.

---

## Pattern 11: ROWNUM and ROW_NUMBER

A small but common gotcha. Oracle's `ROWNUM` is a *pseudo-column* assigned at row-fetch time, before ORDER BY. This causes endless bugs.

### Oracle pitfall

```sql
-- WRONG: returns the first 10 rows fetched, then sorts them — not the top 10
SELECT * FROM customers WHERE ROWNUM <= 10 ORDER BY signup_date DESC;

-- RIGHT: subquery the ordering first
SELECT * FROM (SELECT * FROM customers ORDER BY signup_date DESC)
WHERE ROWNUM <= 10;
```

### Databricks equivalent

```sql
-- Clean and idiomatic
SELECT * FROM customers ORDER BY signup_date DESC LIMIT 10;

-- For "row number per group", use ROW_NUMBER()
SELECT * FROM (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY region ORDER BY signup_date DESC) AS rn
    FROM customers
) WHERE rn <= 10;
```

### The migration risk

When you see `ROWNUM <= N` in legacy PL/SQL, **read the surrounding code carefully** to determine if it's a "give me N arbitrary rows" use case (rare, often a bug) or a "give me the top N by some order" use case (common). The translation differs.

---

## Pattern 12: DECODE / NVL / NVL2 → Spark Equivalents

Oracle's DECODE is concise but non-standard. Spark uses CASE WHEN. NVL becomes COALESCE.

### Translations

```sql
-- Oracle
SELECT DECODE(status, 'A', 'Active', 'I', 'Inactive', 'P', 'Pending', 'Unknown') FROM customers;
SELECT NVL(email, 'no-email@unknown.com') FROM customers;
SELECT NVL2(phone, 'Has Phone', 'No Phone') FROM customers;

-- Spark SQL
SELECT CASE status
    WHEN 'A' THEN 'Active'
    WHEN 'I' THEN 'Inactive'
    WHEN 'P' THEN 'Pending'
    ELSE 'Unknown'
END FROM customers;

SELECT COALESCE(email, 'no-email@unknown.com') FROM customers;
SELECT CASE WHEN phone IS NOT NULL THEN 'Has Phone' ELSE 'No Phone' END FROM customers;
```

### PySpark API

```python
from pyspark.sql.functions import when, col, coalesce, lit

df.withColumn("status_desc",
    when(col("status") == "A", "Active")
    .when(col("status") == "I", "Inactive")
    .when(col("status") == "P", "Pending")
    .otherwise("Unknown"))

df.withColumn("email_safe", coalesce(col("email"), lit("no-email@unknown.com")))
```

The [oracletospark.io](https://oracletospark.io) converter handles these translations automatically for common cases.

---

## Migration playbook: how to use these patterns

When inheriting an Oracle DW migration, my standard sequence is:

**Week 1 — Inventory.** Catalog every PL/SQL package, ODI mapping, materialized view, and GoldenGate flow. Classify each by which pattern from this repo applies (or flag it as "novel" if none do).

**Week 2 — Pilot the hard patterns first.** Pick the 2-3 most complex objects (typically SCD Type 2 dimensions with custom logic, or CDC pipelines) and migrate them end-to-end. The patterns that look easy on paper often have edge cases that only surface in real data — find them early.

**Week 3-N — Production parallel run.** Run Oracle and Databricks side-by-side, with daily reconciliation on row counts, hash totals, and business-key matching. The reconciliation framework itself is reusable across migrations — see [`examples/reconciliation_framework.py`](examples/reconciliation_framework.py).

**Cutover.** Cut reporting workloads to Databricks first (lower risk, no upstream impact). OLTP-adjacent feeds last.

---

## About the author

Madhukar Sripada — 18+ years building enterprise Oracle data platforms (MasterCard, ADP, FDA, Credit Acceptance). Currently leading Oracle-to-Databricks migration programs. Tampa, FL.

- **Live tool:** [oracletospark.io](https://oracletospark.io) — Oracle to Spark SQL converter
- **LinkedIn:** [linkedin.com/in/madhukarsripada](https://www.linkedin.com/in/madhukarsripada)
- **Available for:** Oracle-to-Databricks migration consulting, architecture review, and FT/contract engagements (W2 or C2C)

---

## License

MIT — see [LICENSE](LICENSE). Use these patterns freely in your migrations. If they save you time, a star on the repo and a link back to [oracletospark.io](https://oracletospark.io) is appreciated.
