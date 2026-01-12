import pytest
import pandas as pd
from src.transforms import normalize_orders, deduplicate_orders, enrich_orders

# s
@pytest.fixture
def mock_orders():
    """Simulate the execution when there is case well formed and another one malformed"""
    return [
        {
            "order_id": "o_1",
            "user_id": "u_1",
            "amount": 100.0,
            "created_at": "2025-08-20T10:00:00Z",
            "items": [{"sku": "p_1", "qty": 1, "price": 100.0}],
            "metadata": {"source": "api"}
        },
        {
            "order_id": "o_bad",
            "user_id": "u_2",
            "created_at": None, # Malformed: no date
            "items": []
        }
    ]

def test_normalize_orders_filters_bad_records(mock_orders):
    # Execution
    df = normalize_orders(mock_orders)
    
    # Validaciones
    assert len(df) == 1
    assert df.iloc[0]["order_id"] == "o_1"
    assert "order_amount" in df.columns

def test_deduplicate_orders_keeps_latest():
    # rows with the smae id ut different date
    data = [
        {"order_id": "o_1", "sku": "p_1", "created_at": "2025-01-01"},
        {"order_id": "o_1", "sku": "p_1", "created_at": "2025-01-02"} 
    ]
    df = pd.DataFrame(data)
    
    df_clean = deduplicate_orders(df)
    
    assert len(df_clean) == 1
    assert df_clean.iloc[0]["created_at"] == pd.Timestamp("2025-01-02")

def test_enrich_orders():
    # Setup
    df_orders = pd.DataFrame([{
        "user_id": "u_1", 
        "sku": "p_1"
    }])
    df_users = pd.DataFrame([{
        "user_id": "u_1", 
        "email": "test@test.com"
    }])
    df_products = pd.DataFrame([{
        "sku": "p_1", 
        "name": "Laptop"
    }])
    
    # execution
    df_enriched = enrich_orders(df_orders, df_users, df_products)
    
    # validation
    assert "email" in df_enriched.columns
    assert "name" in df_enriched.columns
    assert df_enriched.iloc[0]["email"] == "test@test.com"