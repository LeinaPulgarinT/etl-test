import pandas as pd
from src.load import write_curated

def test_write_curated_idempotency(tmp_path):
    # 1. data for the first execution
    df_1 = pd.DataFrame([{
        "order_id": "o_1", "sku": "p_1", 
        "created_at_order": pd.Timestamp("2025-08-20"), "order_amount": 100.0
    }])
    
    # 2. execute first time in tmp folder
    write_curated(df_1, base_output_path=tmp_path)
    
    # 3. data of the second execution (same data + some new)
    df_2 = pd.DataFrame([
        {"order_id": "o_1", "sku": "p_1", "created_at_order": pd.Timestamp("2025-08-20"), "order_amount": 100.0},
        {"order_id": "o_2", "sku": "p_2", "created_at_order": pd.Timestamp("2025-08-20"), "order_amount": 50.0}
    ])
    
    # 4. execute second time on the same folder
    write_curated(df_2, base_output_path=tmp_path)
    
    # 5. Validation: file must have 2 rows not 3 
    file_path = tmp_path / "dt=2025-08-20" / "fact_order.csv"
    result_df = pd.read_csv(file_path)
    
    assert len(result_df) == 2, "Idempotency failed: There are duplicated rows"
    assert list(result_df["order_id"]) == ["o_1", "o_2"]

def test_write_raw_creates_file(tmp_path):
    from src.load import write_raw
    
    mock_orders = [{"id": 1}]
    output_dir = tmp_path / "raw"
    
    write_raw(mock_orders, output_dir=str(output_dir), filename="test.json")
    
    assert (output_dir / "test.json").exists()