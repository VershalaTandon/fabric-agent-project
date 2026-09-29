# Sales Analytics Architecture

## Overview

The project uses a Bronze → Silver → Gold Lakehouse architecture.

## Data Flow

```text
CSV Source
    |
    v
OneLake / Fabric Lakehouse
    |
    v
Bronze: raw_sales
    |
    v
PySpark Transformation
    |
    v
Silver: sales_clean
    |
    v
Gold Aggregation
    |
    v
daily_sales_summary
    |
    v
Analytics / ChatBot