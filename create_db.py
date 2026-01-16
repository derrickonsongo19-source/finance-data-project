import duckdb
import os

# Create DuckDB database
db_path = 'data/gold/finance.db'
if os.path.exists(db_path):
    print(f'Database already exists at {db_path}')
else:
    print(f'Creating DuckDB database at {db_path}')
    conn = duckdb.connect(db_path)
    
    # Create schema first
    conn.execute('CREATE SCHEMA IF NOT EXISTS gold')
    
    # Create transactions table
    conn.execute('''
    CREATE TABLE IF NOT EXISTS gold.transactions (
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
        ('2024-01-15', -45.50, 'Grocery Store', 'food', 'Checking'),
        ('2024-01-16', -25.00, 'Uber Ride', 'transport', 'Checking'),
        ('2024-01-17', 2000.00, 'Salary', 'income', 'Checking'),
        ('2024-01-18', -12.99, 'Netflix', 'entertainment', 'Credit Card'),
        ('2024-01-19', -89.99, 'Amazon Purchase', 'shopping', 'Credit Card'),
        ('2024-01-20', -75.00, 'Electric Bill', 'utilities', 'Checking'),
        ('2024-01-21', -35.00, 'Restaurant', 'food', 'Credit Card'),
        ('2024-01-22', -15.00, 'Bus Pass', 'transport', 'Checking'),
        ('2024-01-23', -9.99, 'Spotify', 'entertainment', 'Credit Card'),
        ('2024-01-24', 500.00, 'Freelance Work', 'income', 'Savings'),
    ]
    
    for i, (date, amount, desc, category, account) in enumerate(sample_data, 1):
        conn.execute(f'''
        INSERT INTO gold.transactions (id, date, amount, description, category, account)
        VALUES ({i}, '{date}', {amount}, '{desc}', '{category}', '{account}')
        ''')
    
    print(f'Inserted {len(sample_data)} sample transactions')
    
    # Verify data
    result = conn.execute('SELECT COUNT(*) FROM gold.transactions').fetchone()
    print(f'Total transactions in database: {result[0]}')
    
    conn.close()
    print('Database created successfully!')
