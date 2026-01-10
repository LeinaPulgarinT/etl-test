import json
import logging
import os

logger = logging.getLogger(__name__)

def write_raw(orders, output_dir="output/raw", filename="api_orders.json"):
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


def write_curated(data):
    # Implement the logic to write data to the Curated stage
    pass