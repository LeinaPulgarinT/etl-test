import pandas as pd

def normalize_orders(orders: list) -> pd.DataFrame:

    records = []

    for order in orders:
        if not order.get("created_at"):
            continue # if there is not create_dt it is an invalid record

        items = order.get("items", [])

        if not items:
            continue # Same here, we need the items, if they are not is an invalid record

        for item in items:
            records.append(
                {
                    "order_id": order["order_id"],
                    "user_id": order["user_id"],
                    "sku": item.get("sku"),
                    "qty": item.get("qty", 0),
                    "item_price": item.get("price", 0.0),
                    "order_amount": order.get("amount", 0.0),
                    "currency": order.get("currency"),
                    "created_at": order["created_at"],
                    "source": order.get("metadata", {}).get("source"),
                    "promo": order.get("metadata", {}).get("promo"),
                }
            )
    return pd.DataFrame(records)


def deduplicate_orders(df: pd.DataFrame, date_column="created_at") -> pd.DataFrame:
    df[date_column] = pd.to_datetime(df[date_column])
    return (
        df.sort_values(date_column).drop_duplicates(
            subset=["order_id", "sku"], keep="last"
        )
    )

def enrich_orders(df, users, products):
    df = df.merge(users, on="user_id", how="left", suffixes=("_order", "_user"))
    df = df.merge(products, on="sku", how="left")
    return df