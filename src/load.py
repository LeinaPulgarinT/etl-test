import os
import json
import logging
import pandas as pd

from datetime import date
from pathlib import Path

from src.transforms import deduplicate_orders

logger = logging.getLogger(__name__)

current_date = date.today().strftime("%Y%m%d")

def write_raw(orders, output_dir="output/raw", filename=f"api_orders_{current_date}.json"):
    try:
        # create raw folder if this does not exist
        os.makedirs(output_dir, exist_ok=True)

        file_path = os.path.join(output_dir, filename)

        with open(file_path, "w", encoding="utf-8") as output_file:
            json.dump(orders, output_file, indent=4, ensure_ascii=False)

        logger.info(f"RAW data written to {file_path}")

    except Exception as e:
        logger.exception("Error writing RAW data")
        raise


def write_curated(df: pd.DataFrame, base_output_path: str = "output/curated"):
    df["dt"] = df["created_at_order"].dt.date.astype(str)
    base_path = Path(base_output_path)

    for dt, partition_df in df.groupby("dt"):
        partition_path = base_path / f"dt={dt}"
        file_path = partition_path / "fact_order.csv"

        if file_path.exists():
            # Idempotency here: Read the ppreviuos data to not lose it and to not duplicate it
            existing_df = pd.read_csv(file_path)
            # Union and deduplication to not have conflicts
            combined_df = pd.concat([existing_df, partition_df])
            final_df = deduplicate_orders(combined_df, date_column="created_at_order") 
        else:
            final_df = partition_df

        partition_path.mkdir(parents=True, exist_ok=True)
        final_df.drop(columns=["dt"]).to_csv(file_path, index=False)
        logger.info(f"Partition {dt} updated/created at {file_path}")