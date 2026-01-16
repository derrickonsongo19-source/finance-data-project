import psycopg2
import polars as pl

# Read CSV using Polars
df = pl.read_csv('bank_statement.csv')

# Connect to PostgreSQL
conn = psycopg2.connect(
    host="localhost",
    port=5432,
    database="postgres",
    user="postgres",
    password="mysecretpassword"
)
cursor = conn.cursor()

# Insert each row using Polars iter_rows
for row in df.iter_rows(named=True):
    cursor.execute(
        "INSERT INTO transactions (date, amount, category, account) VALUES (%s, %s, %s, %s)",
        (row['date'], row['amount'], row['category'], row['account'])
    )

conn.commit()
conn.close()
print(f"✅ Loaded {df.height} rows from CSV successfully!")
