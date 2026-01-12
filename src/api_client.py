import json
import os
import time
import logging

logger = logging.getLogger(__name__)


def fetch_orders(since=None, retries=3) -> list:
    base_dir = os.path.dirname(os.path.abspath(__file__))
    json_path = os.path.normpath(
        os.path.join(base_dir, "..", "sample_data", "api_orders.json")
        )
    
    for attempt in range(1, retries + 1):
        try:
            with open(json_path, 'r') as file:
                orders = json.load(file)
                
            if since:
                orders = [
                    order for order in orders
                    if order.get('created_at') and order['created_at'] >= since
                ]
            return orders
        except Exception as e:
            logger.error(f"Attempt {attempt} failed reading API mock: {e}")
            time.sleep(1)
                
    raise RuntimeError("Failed to fetch orders after multiple retries.")


if __name__ == "__main__":
    print(fetch_orders(since="2023-01-01T00:00:00Z"))
