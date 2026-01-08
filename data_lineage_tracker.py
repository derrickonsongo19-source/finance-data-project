import json
from datetime import datetime
from pathlib import Path

class DataLineageTracker:
    def __init__(self, log_file="data_lineage.json"):
        self.log_file = log_file
        self.ensure_log_file()
    
    def ensure_log_file(self):
        """Create log file if it doesn't exist."""
        if not Path(self.log_file).exists():
            with open(self.log_file, 'w') as f:
                json.dump([], f)
    
    def log_transformation(self, source, transformation, destination, row_count=None):
        """Log a data transformation step."""
        entry = {
            "timestamp": datetime.now().isoformat(),
            "source": source,
            "transformation": transformation,
            "destination": destination,
            "row_count": row_count
        }
        
        # Read existing logs
        with open(self.log_file, 'r') as f:
            logs = json.load(f)
        
        # Add new entry
        logs.append(entry)
        
        # Write back
        with open(self.log_file, 'w') as f:
            json.dump(logs, f, indent=2)
        
        print(f"Lineage logged: {source} → {destination}")
    
    def get_lineage(self):
        """Get all lineage logs."""
        with open(self.log_file, 'r') as f:
            return json.load(f)

# Example usage
if __name__ == "__main__":
    tracker = DataLineageTracker()
    
    # Example logs for your pipeline
    tracker.log_transformation(
        source="bank_statement.csv",
        transformation="bronze_layer.py: raw ingestion",
        destination="data/bronze/transactions_raw.parquet",
        row_count=150
    )
    
    tracker.log_transformation(
        source="data/bronze/transactions_raw.parquet",
        transformation="silver_layer.py: clean & type conversion",
        destination="data/silver/transactions_clean.parquet",
        row_count=145
    )
    
    print("\nCurrent lineage:")
    for log in tracker.get_lineage():
        print(f"{log['timestamp']}: {log['source']} → {log['destination']}")
