#!/usr/bin/env python3
"""Test data contract validation independently."""
import polars as pl
from data_contracts import DataContract, ColumnContract

# Load the cleaned data
df = pl.read_parquet('data/silver/transactions_clean.parquet')
print("Data loaded successfully")
print(f"Shape: {df.shape}")
print(f"Columns: {df.columns}")
print(f"Schema: {df.schema}")

# Create a contract matching our data
contract = DataContract(
    contract_name="test_contract",
    columns=[
        ColumnContract(name="transaction_id", dtype="Utf8", nullable=False),
        ColumnContract(name="amount", dtype="Float64", nullable=False, min_value=0.01),
        ColumnContract(name="date", dtype="Date", nullable=False),
        ColumnContract(name="category", dtype="Utf8", nullable=True),
        ColumnContract(name="account_id", dtype="Utf8", nullable=False)
    ]
)

# Validate
result = contract.validate_dataframe(df)
print(f"\nValidation Result:")
print(f"  Valid: {result['valid']}")
print(f"  Errors: {result['errors']}")
print(f"  Row Count: {result['row_count']}")

# Test with invalid data
print("\n--- Testing with invalid data ---")
invalid_df = df.with_columns(
    pl.col("amount").cast(pl.Int64)  # Wrong type
)
invalid_result = contract.validate_dataframe(invalid_df)
print(f"  Valid: {invalid_result['valid']}")
print(f"  Errors: {invalid_result['errors'][:2]}")  # Show first 2 errors

print("\n✅ Data contract testing complete!")
