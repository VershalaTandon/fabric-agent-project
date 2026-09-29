# Sales Data Model

## Source Dataset

The source CSV contains:

| Column | Type | Description |
|---|---|---|
| sale_id | Integer | Unique sale identifier |
| sale_date | Date | Date of transaction |
| product | String | Product name |
| region | String | Sales region |
| quantity | Integer | Number of units |
| unit_price | Decimal | Price per unit |

## Silver Table

Table:

```text
sales_clean