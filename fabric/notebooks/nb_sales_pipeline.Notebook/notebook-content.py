# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {}
# META }

# MARKDOWN ********************

# # Daily sales pipeline: Bronze → Silver → Gold
#
# Ingests `sales.csv` from the default Lakehouse into Delta tables:
#
# | Layer | Table | Write strategy |
# |---|---|---|
# | Bronze | `raw_sales` | Overwrite rows for the source file (`replaceWhere`) |
# | Quarantine | `sales_quarantine` | Overwrite rows for the source file (`replaceWhere`) |
# | Silver | `sales_clean` | `MERGE` on `sale_id` |
# | Gold | `daily_sales_summary` | Full overwrite recomputed from Silver |
#
# Every write is idempotent: re-running the notebook for the same file leaves all tables unchanged.
#
# Attach the target Lakehouse as the notebook's default Lakehouse before running.

# PARAMETERS CELL ********************

source_path = "Files/landing/sales/sales.csv"

bronze_table = "raw_sales"
silver_table = "sales_clean"
quarantine_table = "sales_quarantine"
gold_table = "daily_sales_summary"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql import types as T

SOURCE_COLUMNS = ["sale_id", "sale_date", "product", "region", "quantity", "unit_price"]

# Bronze keeps every source value as a string so nothing is lost before validation.
RAW_SCHEMA = T.StructType([T.StructField(c, T.StringType(), True) for c in SOURCE_COLUMNS])

DATE_PATTERN = r"^\d{4}-\d{2}-\d{2}$"


def read_source_csv(spark: SparkSession, path: str) -> DataFrame:
    return spark.read.option("header", "true").schema(RAW_SCHEMA).csv(path)


def to_bronze(raw_df: DataFrame, source_file: str) -> DataFrame:
    """Add ingestion metadata to the raw records without altering source values."""
    return raw_df.select(
        *SOURCE_COLUMNS,
        F.lit(source_file).alias("_source_file"),
        F.current_timestamp().alias("_ingested_at"),
    )


def validate_sales(bronze_df: DataFrame) -> tuple[DataFrame, DataFrame]:
    """Split Bronze records into (valid, rejected).

    Valid records are typed and carry total_amount = quantity * unit_price.
    Rejected records keep their raw values plus a rejection_reason.
    """
    typed = bronze_df.select(
        "*",
        F.expr("try_cast(trim(sale_id) AS BIGINT)").alias("_sale_id"),
        F.when(
            F.trim("sale_date").rlike(DATE_PATTERN),
            F.expr("try_cast(trim(sale_date) AS DATE)"),
        ).alias("_sale_date"),
        F.expr("try_cast(trim(quantity) AS INT)").alias("_quantity"),
        F.expr("try_cast(trim(unit_price) AS DECIMAL(18,2))").alias("_unit_price"),
    )

    reasons = F.array_compact(
        F.array(
            F.when(F.col("_sale_id").isNull(), F.lit("sale_id is null or not an integer")),
            F.when(F.col("_sale_date").isNull(), F.lit("sale_date is not a valid yyyy-MM-dd date")),
            F.when(
                F.col("_quantity").isNull() | (F.col("_quantity") <= 0),
                F.lit("quantity must be an integer > 0"),
            ),
            F.when(
                F.col("_unit_price").isNull() | (F.col("_unit_price") <= 0),
                F.lit("unit_price must be a number > 0"),
            ),
        )
    )
    checked = typed.withColumn("_reasons", reasons)

    # Exact duplicate rows collapse to one; a sale_id that still appears more than once
    # has conflicting values and cannot be merged deterministically, so it is quarantined.
    passed = checked.filter(F.size("_reasons") == 0).dropDuplicates(SOURCE_COLUMNS)
    passed = passed.withColumn("_dup_count", F.count("*").over(Window.partitionBy("_sale_id")))

    valid = passed.filter(F.col("_dup_count") == 1).select(
        F.col("_sale_id").alias("sale_id"),
        F.col("_sale_date").alias("sale_date"),
        F.trim("product").alias("product"),
        F.trim("region").alias("region"),
        F.col("_quantity").alias("quantity"),
        F.col("_unit_price").alias("unit_price"),
        (F.col("_quantity") * F.col("_unit_price")).cast("decimal(18,2)").alias("total_amount"),
        "_source_file",
        "_ingested_at",
    )

    duplicates = passed.filter(F.col("_dup_count") > 1).withColumn(
        "_reasons", F.array(F.lit("duplicate sale_id with conflicting values"))
    )
    rejected = (
        checked.filter(F.size("_reasons") > 0)
        .unionByName(duplicates.drop("_dup_count"))
        .select(
            *SOURCE_COLUMNS,
            F.array_join("_reasons", "; ").alias("rejection_reason"),
            "_source_file",
            "_ingested_at",
        )
    )
    return valid, rejected


def aggregate_daily_sales(silver_df: DataFrame) -> DataFrame:
    return silver_df.groupBy("sale_date", "region").agg(
        F.sum("quantity").alias("total_quantity"),
        F.sum("total_amount").cast("decimal(18,2)").alias("total_sales"),
    )


def overwrite_source_file_rows(spark: SparkSession, df: DataFrame, table: str, source_file: str) -> None:
    """Replace only the rows previously loaded from source_file, so re-runs never duplicate."""
    writer = df.write.format("delta")
    if spark.catalog.tableExists(table):
        escaped = source_file.replace("'", "''")
        writer = writer.mode("overwrite").option("replaceWhere", f"_source_file = '{escaped}'")
    else:
        writer = writer.mode("errorifexists")
    writer.saveAsTable(table)


def merge_into_silver(spark: SparkSession, valid_df: DataFrame, table: str) -> None:
    if not spark.catalog.tableExists(table):
        valid_df.write.format("delta").saveAsTable(table)
        return
    (
        DeltaTable.forName(spark, table)
        .alias("t")
        .merge(valid_df.alias("s"), "t.sale_id = s.sale_id")
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )


def write_gold(spark: SparkSession, silver_table_name: str, table: str) -> None:
    gold = aggregate_daily_sales(spark.read.table(silver_table_name))
    gold.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(table)


def validate_pipeline(
    spark: SparkSession,
    source_file: str,
    bronze: str,
    silver: str,
    quarantine: str,
    gold: str,
) -> dict:
    """Post-run reconciliation checks. Raises AssertionError if any check fails."""
    bronze_df = spark.read.table(bronze).filter(F.col("_source_file") == source_file)
    silver_df = spark.read.table(silver)
    quarantine_df = spark.read.table(quarantine).filter(F.col("_source_file") == source_file)
    gold_df = spark.read.table(gold)

    bronze_rows = bronze_df.count()
    bronze_distinct = bronze_df.select(*SOURCE_COLUMNS).distinct().count()
    expected_valid, expected_rejected = validate_sales(bronze_df)
    valid_rows = expected_valid.count()
    quarantine_rows = quarantine_df.count()
    missing_from_silver = expected_valid.join(silver_df, "sale_id", "left_anti").count()

    duplicate_ids = silver_df.groupBy("sale_id").count().filter("count > 1").count()
    invalid_silver = silver_df.filter(
        F.col("sale_id").isNull()
        | F.col("sale_date").isNull()
        | (F.col("quantity") <= 0)
        | (F.col("unit_price") <= 0)
        | (F.col("total_amount") != F.col("quantity") * F.col("unit_price"))
    ).count()

    silver_totals = silver_df.agg(F.sum("quantity"), F.sum("total_amount")).first()
    gold_totals = gold_df.agg(F.sum("total_quantity"), F.sum("total_sales")).first()
    gold_duplicate_keys = gold_df.groupBy("sale_date", "region").count().filter("count > 1").count()

    assert bronze_rows > 0, f"{bronze} has no rows for {source_file}"
    assert quarantine_rows == expected_rejected.count(), f"{quarantine} does not match rejected records"
    # Exact duplicate valid rows collapse to one, so the split reconciles against distinct rows.
    assert bronze_distinct <= valid_rows + quarantine_rows <= bronze_rows, "Records lost or duplicated"
    assert missing_from_silver == 0, f"{missing_from_silver} valid records are missing from {silver}"
    assert duplicate_ids == 0, f"{silver} has duplicate sale_id values"
    assert invalid_silver == 0, f"{silver} contains {invalid_silver} invalid records"
    assert gold_duplicate_keys == 0, f"{gold} has duplicate (sale_date, region) keys"
    assert silver_totals[0] == gold_totals[0], "Gold total_quantity does not match Silver"
    assert silver_totals[1] == gold_totals[1], "Gold total_sales does not match Silver"

    return {
        "bronze_rows": bronze_rows,
        "valid_rows": valid_rows,
        "quarantined_rows": quarantine_rows,
        "silver_rows_total": silver_df.count(),
        "gold_rows": gold_df.count(),
    }


def run_pipeline(
    spark: SparkSession,
    source: str,
    bronze: str = "raw_sales",
    silver: str = "sales_clean",
    quarantine: str = "sales_quarantine",
    gold: str = "daily_sales_summary",
) -> dict:
    bronze_df = to_bronze(read_source_csv(spark, source), source)
    overwrite_source_file_rows(spark, bronze_df, bronze, source)

    # Read back from the Delta table so Silver is built from exactly what Bronze stored.
    stored_bronze = spark.read.table(bronze).filter(F.col("_source_file") == source)
    valid_df, rejected_df = validate_sales(stored_bronze)
    overwrite_source_file_rows(spark, rejected_df, quarantine, source)
    merge_into_silver(spark, valid_df, silver)
    write_gold(spark, silver, gold)

    return validate_pipeline(spark, source, bronze, silver, quarantine, gold)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Guarded so tests can import the functions above without running the pipeline.
# In a Fabric notebook session __name__ is "__main__" and `spark` is predefined.
if __name__ == "__main__":
    summary = run_pipeline(
        spark,
        source_path,
        bronze=bronze_table,
        silver=silver_table,
        quarantine=quarantine_table,
        gold=gold_table,
    )
    print(summary)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
