import psycopg2
import random
from datetime import datetime, timedelta

# Connect to PostgreSQL (port 5433)
conn = psycopg2.connect(
    host="localhost",
    port=5433,
    database="postgres",
    user="postgres",
    password="mysecretpassword"
)
cursor = conn.cursor()

print("Setting up PostgreSQL for CDC...")

# 1. Create transactions table
cursor.execute("""
    DROP TABLE IF EXISTS transactions CASCADE;
    CREATE TABLE transactions (
        id SERIAL PRIMARY KEY,
        date DATE NOT NULL,
        amount DECIMAL(10, 2) NOT NULL,
        category VARCHAR(50),
        account VARCHAR(50)
    );
""")
print("✅ Created transactions table")

# 2. Insert sample data
accounts = ['Checking_1234', 'Savings_5678', 'CreditCard_9012']
categories = ['Food', 'Shopping', 'Transport', 'Bills', 'Entertainment', 'Income']

sample_transactions = []
for i in range(20):
    date = datetime.now() - timedelta(days=random.randint(0, 30))
    account = random.choice(accounts)
    category = random.choice(categories)
    amount = random.uniform(-500, 2000)
    
    sample_transactions.append((
        date.date(),
        round(amount, 2),
        category,
        account
    ))

cursor.executemany(
    "INSERT INTO transactions (date, amount, category, account) VALUES (%s, %s, %s, %s)",
    sample_transactions
)

conn.commit()
print(f"✅ Inserted {len(sample_transactions)} sample transactions")

# 3. Show sample data
cursor.execute("""
    SELECT account, COUNT(*) as tx_count, SUM(amount) as balance
    FROM transactions
    GROUP BY account
    ORDER BY account
""")

print("\n📊 Account Summary:")
for row in cursor.fetchall():
    print(f"   {row[0]}: {row[1]} transactions, Balance: ${row[2]:.2f}")

cursor.close()
conn.close()
print("\n✅ PostgreSQL setup complete for CDC")
