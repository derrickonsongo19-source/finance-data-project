import polars as pl
import pytest
from datetime import date

def test_remove_duplicates():
    """Test that duplicate rows are removed."""
    # Create test data with duplicates
    df = pl.DataFrame({
        "id": [1, 2, 2, 3, 4],
        "amount": [100.0, 200.0, 200.0, 300.0, 400.0],
        "date": ["2025-01-01", "2025-01-02", "2025-01-02", "2025-01-03", "2025-01-04"]
    })
    
    # Remove duplicates (simulating what silver_layer does)
    df_clean = df.unique()
    
    # Assert no duplicates in id column
    assert df_clean["id"].is_unique().all()
    assert len(df_clean) == 4  # Should have 4 rows after removing 1 duplicate

def test_date_conversion():
    """Test that date strings are converted to date objects."""
    df = pl.DataFrame({
        "date": ["2025-01-01", "2025-01-02", "2025-01-03"]
    })
    
    # Convert string dates to date type
    df = df.with_columns(
        pl.col("date").str.strptime(pl.Date, "%Y-%m-%d")
    )
    
    # Assert all are date type
    assert df["date"].dtype == pl.Date
