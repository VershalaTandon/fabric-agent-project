from datetime import date
from decimal import Decimal

HEADER = "sale_id,sale_date,product,region,quantity,unit_price\n"


def raw_df(spark, pipeline, rows):
    return pipeline.to_bronze(spark.createDataFrame(rows, pipeline.RAW_SCHEMA), "test.csv")


def write_csv(tmp_path, name, lines):
    path = tmp_path / name
    path.write_text(HEADER + "\n".join(lines) + "\n")
    return str(path)


def test_total_amount(spark, pipeline):
    df = raw_df(
        spark,
        pipeline,
        [
            ("1001", "2026-09-01", "Laptop", "North", "2", "65000"),
            ("1002", "2026-09-01", "Phone", "South", "3", "25000.25"),
        ],
    )
    valid, rejected = pipeline.validate_sales(df)
    totals = {r.sale_id: r.total_amount for r in valid.collect()}

    assert rejected.count() == 0
    assert totals == {1001: Decimal("130000.00"), 1002: Decimal("75000.75")}


def test_invalid_records_are_quarantined_with_reason(spark, pipeline):
    df = raw_df(
        spark,
        pipeline,
        [
            ("1001", "2026-09-01", "Laptop", "North", "2", "65000"),
            (None, "2026-09-01", "Laptop", "North", "1", "65000"),
            ("abc", "2026-09-01", "Laptop", "North", "1", "65000"),
            ("1003", "2026-02-30", "Phone", "West", "2", "25000"),
            ("1004", "09/01/2026", "Phone", "West", "2", "25000"),
            ("1005", None, "Phone", "West", "2", "25000"),
            ("1006", "2026-09-03", "Laptop", "East", "0", "65000"),
            ("1007", "2026-09-03", "Laptop", "East", "-1", "65000"),
            ("1008", "2026-09-03", "Tablet", "West", "2", "0"),
            ("1009", "2026-09-03", "Tablet", "West", "2", "-5"),
            ("1010", "2026-09-03", "Tablet", "West", "x", "abc"),
        ],
    )
    valid, rejected = pipeline.validate_sales(df)
    reasons = {r.sale_id: r.rejection_reason for r in rejected.collect()}

    assert [r.sale_id for r in valid.collect()] == [1001]
    assert len(reasons) == 10
    assert "sale_id" in reasons[None]
    assert "sale_id" in reasons["abc"]
    for bad_date in ("1003", "1004", "1005"):
        assert "sale_date" in reasons[bad_date]
    for bad_qty in ("1006", "1007"):
        assert "quantity" in reasons[bad_qty]
    for bad_price in ("1008", "1009"):
        assert "unit_price" in reasons[bad_price]
    assert "quantity" in reasons["1010"] and "unit_price" in reasons["1010"]
    # Raw values are preserved in quarantine for investigation.
    assert rejected.filter("sale_id = '1004'").first().sale_date == "09/01/2026"


def test_duplicate_sale_ids(spark, pipeline):
    df = raw_df(
        spark,
        pipeline,
        [
            ("1001", "2026-09-01", "Laptop", "North", "2", "65000"),
            ("1001", "2026-09-01", "Laptop", "North", "2", "65000"),
            ("1002", "2026-09-01", "Phone", "South", "3", "25000"),
            ("1002", "2026-09-01", "Phone", "South", "4", "25000"),
        ],
    )
    valid, rejected = pipeline.validate_sales(df)

    assert [r.sale_id for r in valid.collect()] == [1001]
    assert rejected.count() == 2
    assert {r.rejection_reason for r in rejected.collect()} == {"duplicate sale_id with conflicting values"}


def test_daily_sales_aggregation(spark, pipeline):
    df = raw_df(
        spark,
        pipeline,
        [
            ("1001", "2026-09-01", "Laptop", "North", "2", "65000"),
            ("1002", "2026-09-01", "Laptop", "North", "1", "65000"),
            ("1003", "2026-09-01", "Phone", "South", "3", "25000"),
            ("1004", "2026-09-02", "Phone", "North", "1", "25000"),
        ],
    )
    valid, _ = pipeline.validate_sales(df)
    result = {(r.sale_date, r.region): (r.total_quantity, r.total_sales) for r in pipeline.aggregate_daily_sales(valid).collect()}

    assert result == {
        (date(2026, 9, 1), "North"): (3, Decimal("195000.00")),
        (date(2026, 9, 1), "South"): (3, Decimal("75000.00")),
        (date(2026, 9, 2), "North"): (1, Decimal("25000.00")),
    }


def snapshot(spark, tables):
    return {
        layer: sorted(
            (tuple(v for k, v in row.asDict().items() if k != "_ingested_at") for row in spark.read.table(name).collect()),
            key=repr,
        )
        for layer, name in tables.items()
    }


def test_end_to_end_pipeline(spark, pipeline, tables, sample_csv):
    summary = pipeline.run_pipeline(spark, sample_csv, **tables)

    # Bronze holds every raw record, unmodified, as strings.
    assert summary["bronze_rows"] == 13
    bronze = spark.read.table(tables["bronze"])
    assert bronze.filter("sale_id IS NULL").count() == 1
    assert bronze.filter("sale_date = 'not-a-date'").count() == 1
    assert all(f.dataType.simpleString() == "string" for f in bronze.schema if f.name in pipeline.SOURCE_COLUMNS)

    # 7 invalid rows quarantined; 6 valid rows of which 2 are exact duplicates -> 5 Silver rows.
    assert summary["quarantined_rows"] == 7
    silver = spark.read.table(tables["silver"])
    assert sorted(r.sale_id for r in silver.collect()) == [1001, 1002, 1003, 1004, 1005]
    assert silver.filter("sale_id = 1004").first().total_amount == Decimal("120002.00")

    gold = {(r.sale_date, r.region): (r.total_quantity, r.total_sales) for r in spark.read.table(tables["gold"]).collect()}
    assert gold == {
        (date(2026, 9, 1), "North"): (3, Decimal("195000.00")),
        (date(2026, 9, 1), "South"): (3, Decimal("75000.00")),
        (date(2026, 9, 2), "East"): (4, Decimal("120002.00")),
        (date(2026, 9, 2), "South"): (1, Decimal("25000.00")),
    }


def test_rerun_does_not_duplicate(spark, pipeline, tables, sample_csv):
    pipeline.run_pipeline(spark, sample_csv, **tables)
    first = snapshot(spark, tables)

    pipeline.run_pipeline(spark, sample_csv, **tables)
    pipeline.run_pipeline(spark, sample_csv, **tables)

    assert snapshot(spark, tables) == first


def test_incremental_daily_files(spark, pipeline, tables, tmp_path):
    day1 = write_csv(tmp_path, "sales_2026-09-01.csv", ["1001,2026-09-01,Laptop,North,2,65000", ",2026-09-01,Phone,South,1,25000"])
    day2 = write_csv(tmp_path, "sales_2026-09-02.csv", ["1002,2026-09-02,Phone,South,3,25000"])

    pipeline.run_pipeline(spark, day1, **tables)
    pipeline.run_pipeline(spark, day2, **tables)
    pipeline.run_pipeline(spark, day1, **tables)  # re-delivery of day 1

    assert spark.read.table(tables["bronze"]).count() == 3
    assert spark.read.table(tables["quarantine"]).count() == 1
    assert sorted(r.sale_id for r in spark.read.table(tables["silver"]).collect()) == [1001, 1002]
    assert spark.read.table(tables["gold"]).count() == 2


def test_corrected_record_updates_silver_and_gold(spark, pipeline, tables, tmp_path):
    path = write_csv(tmp_path, "sales.csv", ["1001,2026-09-01,Laptop,North,2,65000"])
    pipeline.run_pipeline(spark, path, **tables)

    write_csv(tmp_path, "sales.csv", ["1001,2026-09-01,Laptop,North,5,65000"])
    pipeline.run_pipeline(spark, path, **tables)

    silver = spark.read.table(tables["silver"]).collect()
    gold = spark.read.table(tables["gold"]).collect()
    assert len(silver) == 1 and silver[0].quantity == 5
    assert len(gold) == 1 and gold[0].total_sales == Decimal("325000.00")


def test_tables_are_delta(spark, pipeline, tables, sample_csv):
    pipeline.run_pipeline(spark, sample_csv, **tables)
    for name in tables.values():
        detail = spark.sql(f"DESCRIBE DETAIL {name}").first()
        assert detail.format == "delta", name
