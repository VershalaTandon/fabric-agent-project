# Sales Data Model

## Source Dataset

The source CSV (`sales.csv`) contains:

| Column | Type | Description |
|---|---|---|
| sale_id | Integer | Unique sale identifier |
| sale_date | Date | Date of transaction (`yyyy-MM-dd`) |
| product | String | Product name |
| region | String | Sales region |
| quantity | Integer | Number of units |
| unit_price | Decimal | Price per unit |

A sample file with valid and invalid rows is in `data/sample/sales.csv`.

## Bronze Table

Table: `raw_sales` (Delta)

Raw records exactly as received. All source columns are stored as strings so no value is lost before validation.

| Column | Type | Description |
|---|---|---|
| sale_id … unit_price | String | Source columns, unmodified |
| _source_file | String | Path of the file the record was loaded from |
| _ingested_at | Timestamp | Load time |

## Quarantine Table

Table: `sales_quarantine` (Delta)

Records rejected by Silver validation, with raw values preserved.

| Column | Type | Description |
|---|---|---|
| sale_id … unit_price | String | Source columns, unmodified |
| rejection_reason | String | All failed rules, separated by `; ` |
| _source_file | String | Source file |
| _ingested_at | Timestamp | Bronze load time |

## Silver Table

Table: `sales_clean` (Delta)

Validated, typed records. One row per `sale_id`.

| Column | Type | Description |
|---|---|---|
| sale_id | BIGINT | Unique sale identifier (merge key) |
| sale_date | DATE | Date of transaction |
| product | STRING | Product name (trimmed) |
| region | STRING | Sales region (trimmed) |
| quantity | INT | Units sold, > 0 |
| unit_price | DECIMAL(18,2) | Price per unit, > 0 |
| total_amount | DECIMAL(18,2) | `quantity * unit_price` |
| _source_file | STRING | Source file of the latest version of the record |
| _ingested_at | TIMESTAMP | Bronze load time |

### Validation rules

A record is rejected to `sales_quarantine` when any of these fail:

| Rule | Rejection reason |
|---|---|
| `sale_id` is present and an integer | `sale_id is null or not an integer` |
| `sale_date` matches `yyyy-MM-dd` and is a real calendar date | `sale_date is not a valid yyyy-MM-dd date` |
| `quantity` is an integer > 0 | `quantity must be an integer > 0` |
| `unit_price` is a number > 0 | `unit_price must be a number > 0` |
| `sale_id` appears once in the file (exact duplicate rows collapse to one) | `duplicate sale_id with conflicting values` |

## Gold Table

Table: `daily_sales_summary` (Delta)

| Column | Type | Description |
|---|---|---|
| sale_date | DATE | Sale date |
| region | STRING | Sales region |
| total_quantity | BIGINT | `SUM(quantity)` |
| total_sales | DECIMAL(18,2) | `SUM(total_amount)` |

One row per `(sale_date, region)`.
