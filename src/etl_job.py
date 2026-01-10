import logging

from src.api_client import fetch_orders
from src.load import write_raw, write_curated


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
)


def run_etl_job():
    # Extract 
    orders = fetch_orders()
    logging.info(f"Fetched {len(orders)} orders from API.")
    
    # Load to RAW stage
    write_raw(orders)
    logging.info("Written orders to RAW stage.")
    
    
    # Transform
    # here add logict to tranform, or call tranformations functions from transforms.py
    logging.info("Transformed data.")

    # Load CURATED stage
    # here add logic to load data, e.g., save to file (create a load module)
    logging.info("Written data to CURATED stage.")
    

if __name__ == "__main__":
    run_etl_job()