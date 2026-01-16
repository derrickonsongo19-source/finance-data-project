import duckdb
import os

db_path = 'data/gold/finance.db'

# Remove existing file if it exists
if os.path.exists(db_path):
    print(f'Removing existing database at {db_path}')
    os.remove(db_path)

print(f'Creating DuckDB database at {db_path}')
conn = duckdb.connect(db_path)

# Create schema
conn.execute('CREATE SCHEMA IF NOT EXISTS gold')

# Create transactions table
conn.execute('''
CREATE TABLE gold.transactions (
    id INTEGER PRIMARY KEY,
    date DATE,
    amount FLOAT,
    description VARCHAR,
    category VARCHAR,
    account VARCHAR,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
)
''')

# Insert sample data
sample_data = [
    (1, '2024-01-15', -45.50, 'Grocery Store', 'food', 'Checking'),
    (2, '2024-01-16', -25.00, 'Uber Ride', 'transport', 'Checking'),
    (3, '2024-01-17', 2000.00, 'Salary', 'income', 'Checking'),
    (4, '2024-01-18', -12.99, 'Netflix', 'entertainment', 'Credit Card'),
    (5, '2024-01-19', -89.99, 'Amazon Purchase', 'shopping', 'Credit Card'),
    (6, '2024-01-20', -75.00, 'Electric Bill', 'utilities', 'Checking'),
    (7, '2024-01-21', -35.00, 'Restaurant', 'food', 'Credit Card'),
    (8, '2024-01-22', -15.00, 'Bus Pass', 'transport', 'Checking'),
    (9, '2024-01-23', -9.99, 'Spotify', 'entertainment', 'Credit Card'),
    (10, '2024-01-24', 500.00, 'Freelance Work', 'income', 'Savings'),
]

for id, date, amount, desc, category, account in sample_data:
    conn.execute(f'''
    INSERT INTO gold.transactions (id, date, amount, description, category, account)
    VALUES ({id}, '{date}', {amount}, '{desc}', '{category}', '{account}')
    ''')

print(f'Inserted {len(sample_data)} sample transactions')

# Verify data
count = conn.execute('SELECT COUNT(*) FROM gold.transactions').fetchone()[0]
print(f'Total transactions in database: {count}')

# Show table structure
print('\nTable structure:')
table_info = conn.execute('DESCRIBE gold.transactions').fetchall()
for col in table_info:
    print(f'  {col[0]}: {col[1]}')

conn.close()
print('\n✓ Database created successfully!')
