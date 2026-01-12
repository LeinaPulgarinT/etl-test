import logging

logger = logging.getLogger(__name__)


def validate_orders(df):
    initial_count = len(df)

    # Not null checks
    df = df.dropna(subset=["order_id", "user_id", "sku"])

    # Numeric checks
    df = df[
        (df["qty"] > 0) &
        (df["item_price"] >= 0) &
        (df["order_amount"] >= 0)
    ]

    # Consistency check
    df = df[df["qty"] * df["item_price"] <= df["order_amount"]]

    # Deduplicate
    df = df.drop_duplicates(subset=["order_id", "sku"])

    final_count = len(df)

    logger.info(
        f"Validation completed. Rows before: {initial_count}, after: {final_count}"
    )

    return df