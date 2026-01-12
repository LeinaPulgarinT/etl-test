import json
import pytest
from unittest.mock import patch, mock_open
from src.api_client import fetch_orders

def test_fetch_orders_success():
    # mock the json answer
    mock_data = '[{"order_id": "o_1", "created_at": "2025-08-20T10:00:00Z"}]'
    
    # Mock the apen file action
    with patch("builtins.open", mock_open(read_data=mock_data)):
        orders = fetch_orders()
        assert len(orders) == 1
        assert orders[0]["order_id"] == "o_1"

def test_fetch_orders_incremental():
    mock_data = json.dumps([
        {"order_id": "o_1", "created_at": "2025-01-01T00:00:00Z"},
        {"order_id": "o_2", "created_at": "2025-01-10T00:00:00Z"}
    ])
    
    with patch("builtins.open", mock_open(read_data=mock_data)):
        #only want the orders since january 5
        orders = fetch_orders(since="2025-01-05")
        assert len(orders) == 1
        assert orders[0]["order_id"] == "o_2"

def test_fetch_orders_retries_and_fails():
    # mock that 'open' raises an exception
    with patch("builtins.open", side_effect=Exception("File not found")):
        with pytest.raises(RuntimeError) as excinfo:
            fetch_orders(retries=2)
        
        assert "Failed to fetch orders" in str(excinfo.value)