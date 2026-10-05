# Sales Analytics Architecture

## Overview

The project uses a Bronze → Silver → Gold Lakehouse architecture, implemented by the Fabric notebook
`fabric/notebooks/nb_sales_pipeline.Notebook`.

## Data Flow

```text
CSV Source (Lakehouse Files/landing/sales/sales.csv)
    |
    v
Bronze: raw_sales                (raw strings + _source_file, _ingested_at)
    |
    v
PySpark validation ---------->   sales_quarantine (rejected rows + rejection_reason)
    |
    v
Silver: sales_clean              (typed, validated, total_amount)
    |
    v
Gold Aggregation
    |
    v
daily_sales_summary              (sale_date, region, total_quantity, total_sales)
    |
    v
Analytics / ChatBot
```

## Idempotency

| Table | Write strategy | Re-run behaviour |
|---|---|---|
| `raw_sales` | `overwrite` with `replaceWhere _source_file = <file>` | Rows for that file are replaced, other files untouched |
| `sales_quarantine` | `overwrite` with `replaceWhere _source_file = <file>` | Same as Bronze |
| `sales_clean` | Delta `MERGE` on `sale_id` (update matched, insert new) | Existing sales are updated in place, never duplicated |
| `daily_sales_summary` | Full `overwrite` recomputed from Silver | Always consistent with Silver |

See [sales-pipeline.md](sales-pipeline.md) for how to run the pipeline and [data-model.md](data-model.md) for schemas.
