# Daily Sales Pipeline

Implements [issue #2](https://github.com/VershalaTandon/fabric-agent-project/issues/2): daily sales
ingestion and Gold aggregation in Microsoft Fabric.

## Fabric artifacts

| Artifact | Path | Purpose |
|---|---|---|
| Notebook `nb_sales_pipeline` | `fabric/notebooks/nb_sales_pipeline.Notebook/` | Bronze → Silver → Gold PySpark pipeline (Fabric Git integration format) |

The notebook creates these Delta tables in its default Lakehouse on first run:
`raw_sales`, `sales_quarantine`, `sales_clean`, `daily_sales_summary` (see [data-model.md](data-model.md)).

## Running in Fabric

1. Sync the workspace with this repository (Fabric Git integration) so `nb_sales_pipeline` appears in the workspace.
2. Open the notebook and attach your Lakehouse as the **default Lakehouse**.
3. Upload the daily file to the Lakehouse, by default at `Files/landing/sales/sales.csv`
   (`data/sample/sales.csv` can be used for a first run).
4. Run all cells, or schedule the notebook / call it from a Data Pipeline Notebook activity.

Parameters (the notebook's parameters cell, overridable from a pipeline):

| Parameter | Default |
|---|---|
| `source_path` | `Files/landing/sales/sales.csv` |
| `bronze_table` | `raw_sales` |
| `silver_table` | `sales_clean` |
| `quarantine_table` | `sales_quarantine` |
| `gold_table` | `daily_sales_summary` |

For daily files with distinct names (e.g. `Files/landing/sales/sales_2026-09-01.csv`) pass `source_path` per run;
each file's Bronze/quarantine rows are tracked separately by `_source_file`.

## Built-in validation

After loading, `validate_pipeline` runs reconciliation checks and fails the run if any break:

- Bronze has rows for the source file.
- Quarantine row count matches the records rejected by validation.
- Valid + rejected records reconcile with Bronze (exact duplicate rows collapse to one).
- Every valid record from the file exists in Silver.
- Silver has no duplicate `sale_id` and no rows that violate the validation rules or the `total_amount` formula.
- Gold has one row per `(sale_date, region)` and its totals equal Silver's totals.

The run prints a summary: `bronze_rows`, `valid_rows`, `quarantined_rows`, `silver_rows_total`, `gold_rows`.

## Local tests

Tests import the notebook source directly (its run cell is guarded by `if __name__ == "__main__"`), so the code
under test is exactly the code deployed to Fabric. They run Spark locally with Delta Lake.

Requirements: Python 3.11 and Java 17.

```bash
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
pytest -v
```

Versions are pinned to Fabric Runtime 1.3 (Spark 3.5, Delta Lake 3.2).

## Known limitations

- If a re-delivered file drops a sale that an earlier delivery loaded, the sale stays in Silver (MERGE does not delete).
- Rules beyond those in issue #2 (e.g. empty `region`/`product`) are not enforced; such rows reach Silver.
- Gold is fully recomputed each run, which is fine at daily-file scale but should become date-scoped for large histories.
