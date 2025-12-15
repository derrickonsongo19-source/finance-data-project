import psycopg2

# Connect to PostgreSQL
conn = psycopg2.connect(
    host="localhost",
    port=5432,
    database="postgres",
    user="postgres",
    password="mysecretpassword"
)
cursor = conn.cursor()

# Insert new transaction
cursor.execute(
    "INSERT INTO transactions (date, amount, category, account) VALUES (%s, %s, %s, %s)",
    ('2025-01-17', -30.00, 'Transport', 'Checking')
)

# Commit and close
conn.commit()
conn.close()

print("✅ Data loaded successfully!")
