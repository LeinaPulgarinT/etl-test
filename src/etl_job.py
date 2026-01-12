import duckdb
import logging
import argparse
import pandas as pd
from pathlib import Path

from src.api_client import fetch_orders
from src.load import write_raw, write_curated
from src.transforms import normalize_orders, deduplicate_orders, enrich_orders
from src.validations import validate_orders

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
)

logger = logging.getLogger(__name__)

BASE_DIR = Path(__file__).resolve().parent.parent

def _get_since_date():
    parser = argparse.ArgumentParser(description="ETL job to process data since a speacific date")
    parser.add_argument(
        "--since",
        type=str, 
        required=False, 
        help="start date format YYYY-MM-DD"
    )
    args = parser.parse_args()
    return args.since



def run_etl_job():
    # Get since date
    since_date = _get_since_date()
    
    # Extract orders from API
    logger.info(" :::::::::::::  Extraction  ::::::::::::::::::")
    if since_date:
        orders = fetch_orders(since=since_date)
        logger.info(f" - Running incremental load since {since_date}")
    else:
        orders = fetch_orders()
        logging.info("Running full load")
    logger.info(" - Fetched %s orders from API.", len(orders))

    # Extract reference data
    products = pd.read_csv(BASE_DIR / "data" / "products.csv")
    logger.info(" - Loaded %s products from CSV.", len(products))

    users = pd.read_csv(BASE_DIR / "data" / "users.csv")
    logger.info(" - Loaded %s users from CSV.", len(users))

    # Load RAW stage
    write_raw(orders)
    logger.info(" - Written orders to RAW stage.")

    logger.info(" :::::::::::::  Transformation  ::::::::::::::::::")
    df_orders = normalize_orders(orders)
    logger.info(" - Normalized orders: %s rows", len(df_orders))
    
    # - Deduplicate orders by order_id
    df_orders = deduplicate_orders(df_orders)
    logger.info(" - After deduplication: %s rows", len(df_orders))

    df_orders = enrich_orders(df=df_orders, users=users, products=products)
    logger.info(" - Enriched orders.")

    df_orders = validate_orders(df_orders)

    logger.info(" :::::::::::::  Load  ::::::::::::::::::")
    write_curated(df=df_orders)
    logger.info(" - Written data to CURATED stage.")


def run_sanity_checks():
    """
    Execute SQL queries to validate the model
    """
    con = duckdb.connect()
    
    try:
        logger.info("--- DuckDB Sanity Check ---")
        res = con.execute("""
            SELECT 
                COUNT(DISTINCT order_id) as total_orders,
                SUM(order_amount) as total_revenue
            FROM read_csv_auto('output/curated/dt=*/fact_order.csv')
        """).df()
        
        logger.info(f"Total Unique Orders: {res['total_orders'][0]}")
        logger.info(f"Total Revenue in Curated: ${res['total_revenue'][0]:,.2f}")
        
    except Exception as e:
        logger.error(f"Could not run DuckDB checks: {e}")


if __name__ == "__main__":
    run_etl_job()
    run_sanity_checks()