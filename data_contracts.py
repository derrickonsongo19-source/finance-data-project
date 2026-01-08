import json
import pandas as pd
import polars as pl
from typing import Dict, List, Any
from dataclasses import dataclass
from datetime import datetime

@dataclass
class ColumnContract:
    name: str
    dtype: str
    nullable: bool = True
    min_value: Any = None
    max_value: Any = None
    allowed_values: List[Any] = None

class DataContract:
    def __init__(self, contract_name: str, columns: List[ColumnContract]):
        self.contract_name = contract_name
        self.columns = {col.name: col for col in columns}
        self.created_at = datetime.now().isoformat()
    
    def validate_dataframe(self, df: pl.DataFrame) -> Dict[str, List[str]]:
        """Validate a Polars DataFrame against this contract."""
        errors = []
        
        # Check all required columns exist
        expected_columns = set(self.columns.keys())
        actual_columns = set(df.columns)
        
        missing_columns = expected_columns - actual_columns
        if missing_columns:
            errors.append(f"Missing columns: {missing_columns}")
        
        extra_columns = actual_columns - expected_columns
        if extra_columns:
            errors.append(f"Extra columns: {extra_columns}")
        
        # Check each column's properties
        for col_name, contract in self.columns.items():
            if col_name not in df.columns:
                continue
                
            col_data = df[col_name]
            
            # Check nullability
            if not contract.nullable and col_data.null_count() > 0:
                errors.append(f"Column '{col_name}' has null values but should not")
            
            # Check data type (simplified)
            if str(col_data.dtype) != contract.dtype:
                errors.append(f"Column '{col_name}' has type {col_data.dtype}, expected {contract.dtype}")
            
            # Check value range for numeric columns
            if contract.min_value is not None and contract.max_value is not None:
                if col_data.min() < contract.min_value or col_data.max() > contract.max_value:
                    errors.append(f"Column '{col_name}' values outside range [{contract.min_value}, {contract.max_value}]")
            
            # Check allowed values
            if contract.allowed_values:
                unique_values = col_data.unique().to_list()
                invalid_values = [v for v in unique_values if v not in contract.allowed_values]
                if invalid_values:
                    errors.append(f"Column '{col_name}' has invalid values: {invalid_values}")
        
        return {
            "contract": self.contract_name,
            "timestamp": datetime.now().isoformat(),
            "valid": len(errors) == 0,
            "errors": errors,
            "row_count": len(df)
        }
    
    def to_json(self) -> str:
        """Serialize contract to JSON."""
        contract_dict = {
            "contract_name": self.contract_name,
            "created_at": self.created_at,
            "columns": [
                {
                    "name": col.name,
                    "dtype": col.dtype,
                    "nullable": col.nullable,
                    "min_value": col.min_value,
                    "max_value": col.max_value,
                    "allowed_values": col.allowed_values
                }
                for col in self.columns.values()
            ]
        }
        return json.dumps(contract_dict, indent=2)
    
    @classmethod
    def from_json(cls, json_str: str):
        """Create contract from JSON."""
        data = json.loads(json_str)
        columns = []
        for col_data in data["columns"]:
            columns.append(ColumnContract(**col_data))
        return cls(data["contract_name"], columns)

# Example: Define a contract for your transactions data
transactions_contract = DataContract(
    contract_name="transactions_silver_contract",
    columns=[
        ColumnContract(name="id", dtype="Int64", nullable=False),
        ColumnContract(name="date", dtype="Date", nullable=False),
        ColumnContract(name="amount", dtype="Float64", nullable=False, min_value=-10000, max_value=100000),
        ColumnContract(
            name="category", 
            dtype="Utf8", 
            nullable=False,
            allowed_values=["Salary", "Shopping", "Transport", "Restaurant", "Coffee", "Utilities", "Entertainment"]
        ),
        ColumnContract(name="account", dtype="Utf8", nullable=False)
    ]
)

if __name__ == "__main__":
    # Save the contract to a file
    with open("transactions_contract.json", "w") as f:
        f.write(transactions_contract.to_json())
    
    print("Data contract saved to 'transactions_contract.json'")
    print("\nContract contents:")
    print(transactions_contract.to_json())
    
    # Example validation (you would use your actual data)
    print("\nTo validate data against this contract:")
    print("1. Load your data: df = pl.read_parquet('data/silver/transactions_clean.parquet')")
    print("2. Validate: result = transactions_contract.validate_dataframe(df)")
    print("3. Check result['valid'] and result['errors']")
