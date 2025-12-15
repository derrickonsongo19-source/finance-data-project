import psycopg2
import pandas as pd

# Read CSV
df = pd.read_csv('bank_statement.csv')

# Connect to PostgreSQL
conn = psycopg2.connect(
    host="localhost",
    port=5432,
    database="postgres",
    user="postgres",
    password="mysecretpassword"
)
cursor = conn.cursor()

# Insert each row
for index, row in df.iterrows():
    cursor.execute(
        "INSERT INTO transactions (date, amount, category, account) VALUES (%s, %s, %s, %s)",
        (row['date'], row['amount'], row['category'], row['account'])
    )

conn.commit()
conn.close()
print(f"✅ Loaded {len(df)} rows from CSV successfully!")
