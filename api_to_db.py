import psycopg2
import requests

# Fetch data from mock API
response = requests.get("http://localhost:8000/transactions")
data = response.json()

# Connect to PostgreSQL
conn = psycopg2.connect(
    host="localhost",
    port=5432,
    database="postgres",
    user="postgres",
    password="mysecretpassword"
)
cursor = conn.cursor()

inserted_count = 0

for transaction in data["transactions"]:
    # Check if transaction already exists (simple deduplication)
    cursor.execute(
        "SELECT id FROM transactions WHERE date = %s AND amount = %s AND category = %s AND account = %s",
        (transaction["date"], transaction["amount"], transaction["description"], transaction["account"])
    )
    
    if cursor.fetchone() is None:  # No duplicate found
        cursor.execute(
            "INSERT INTO transactions (date, amount, category, account) VALUES (%s, %s, %s, %s)",
            (transaction["date"], transaction["amount"], transaction["description"], transaction["account"])
        )
        inserted_count += 1

conn.commit()
conn.close()

if inserted_count > 0:
    print(f"✅ Loaded {inserted_count} new transactions from API!")
else:
    print("⚠️  No new transactions found (all already in database).")
