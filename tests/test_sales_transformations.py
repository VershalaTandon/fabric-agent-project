import pandas as pd

def calculate_total_amount(df):
    df = df.copy()
    df["total_amount"] = df["quantity"] * df["unit_price"]
    return df

def aggregate_daily_sales(df):
    return (
        df.groupby(["sale_date", "region"], as_index=False)
        .agg(
            total_quantity=("quantity", "sum"),
            total_sales=("total_amount", "sum"),
        )
    )

def test_total_amount():
    df = pd.DataFrame(
        {
            "sale_id": [1001, 1002],
            "sale_date": ["2026-09-01", "2026-09-01"],
            "product": ["Laptop", "Phone"],
            "region": ["North", "South"],
            "quantity": [2, 3],
            "unit_price": [65000, 25000],
        }
    )

    result = calculate_total_amount(df)

    assert result.loc[0, "total_amount"] == 130000
    assert result.loc[1, "total_amount"] == 75000

def test_daily_sales_aggregation():
    df = pd.DataFrame(
        {
            "sale_date": [
                "2026-09-01",
                "2026-09-01",
                "2026-09-01",
            ],
            "region": [
                "North",
                "North",
                "South",
            ],
            "quantity": [
                2,
                1,
                3,
            ],
            "total_amount": [
                130000,
                65000,
                75000,
            ],
        }
    )

    result = aggregate_daily_sales(df)

    north = result[(result["sale_date"] == "2026-09-01") & (result["region"] == "North")].iloc[0]

    assert north["total_quantity"] == 3
    assert north["total_sales"] == 195000

def test_invalid_quantity():
    df = pd.DataFrame(
        {
            "quantity": [2, 0, -1],
            "unit_price": [100, 200, 300],
        }
    )

    valid = df[
        (df["quantity"] > 0)
        & (df["unit_price"] > 0)
    ]

    assert len(valid) == 1