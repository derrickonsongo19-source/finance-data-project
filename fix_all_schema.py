import re

with open('api/data_loader.py', 'r') as f:
    content = f.read()

# Fix all occurrences of account → account_id
content = re.sub(r'\baccount\b(?=,|\s|$)', 'account_id', content)

# Remove any remaining 'id' columns from SELECT statements
# This is a more targeted fix for the analytics_transactions view
lines = content.split('\n')
fixed_lines = []
in_sql_block = False
for line in lines:
    if 'CREATE OR REPLACE VIEW analytics_transactions AS' in line:
        in_sql_block = True
    elif in_sql_block and line.strip() == '':
        in_sql_block = False
    
    if in_sql_block and 'id,' in line:
        # Skip the id line
        continue
    elif in_sql_block and 'account_id,' in line:
        # This is fine
        fixed_lines.append(line)
    else:
        fixed_lines.append(line)

content = '\n'.join(fixed_lines)

# Also fix the other problematic SQL blocks
# Let's simplify and use a working SQL statement
simple_sql = '''
        self.conn.execute("""
            CREATE OR REPLACE VIEW analytics_transactions AS
            SELECT 
                date,
                amount,
                ABS(amount) as absolute_amount,
                CASE 
                    WHEN amount < 0 THEN 'debit'
                    WHEN amount > 0 THEN 'credit'
                    ELSE 'zero'
                END as transaction_type,
                category,
                account_id
            FROM silver_transactions
        """)
        
        self.conn.execute("""
            CREATE OR REPLACE VIEW category_summary AS
            SELECT 
                category,
                COUNT(*) as transaction_count,
                SUM(amount) as total_amount,
                AVG(amount) as avg_amount
            FROM silver_transactions
            GROUP BY category
        """)
'''

# Replace the entire setup_analytics_views method
pattern = r'def setup_analytics_views\(self\):.*?(?=def |\Z)'
replacement = '''def setup_analytics_views(self):
        print("🔧 Setting up DuckDB analytics views...")
        
        # Register the Parquet file as a table
        self.conn.execute("""
            CREATE OR REPLACE TABLE silver_transactions AS 
            SELECT * FROM read_parquet('data/silver/transactions_clean.parquet')
        """)
        
        # Create analytics views
        self.conn.execute("""
            CREATE OR REPLACE VIEW analytics_transactions AS
            SELECT 
                date,
                amount,
                ABS(amount) as absolute_amount,
                CASE 
                    WHEN amount < 0 THEN 'debit'
                    WHEN amount > 0 THEN 'credit'
                    ELSE 'zero'
                END as transaction_type,
                category,
                account_id
            FROM silver_transactions
        """)
        
        self.conn.execute("""
            CREATE OR REPLACE VIEW category_summary AS
            SELECT 
                category,
                COUNT(*) as transaction_count,
                SUM(amount) as total_amount,
                AVG(amount) as avg_amount
            FROM silver_transactions
            GROUP BY category
        """)
        
        print("✅ Analytics views created successfully")'''

content = re.sub(pattern, replacement, content, flags=re.DOTALL)

with open('api/data_loader.py', 'w') as f:
    f.write(content)

print("All schema issues fixed in data_loader.py")
