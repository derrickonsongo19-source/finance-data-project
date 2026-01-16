# Read the data_loader.py file
with open('api/data_loader.py', 'r') as f:
    content = f.read()

# Replace the problematic SQL with correct schema
new_content = content.replace("""                id,
                date,
                amount,
                category,
                account,""", """                date,
                amount,
                category,
                account_id,""")

# Also replace the second occurrence
new_content = new_content.replace("""        # Check what columns we actually have in silver_transactions
        cols = self.conn.execute(\"\"\"
            SELECT column_name 
            FROM information_schema.columns 
            WHERE table_name = 'silver_transactions'
        \"\"\").fetchall()
        print(\"Actual columns in silver_transactions:\", cols)""", """        # Create view with actual schema
        print(\"Creating analytics view with actual schema...\")""")

# Write back
with open('api/data_loader.py', 'w') as f:
    f.write(new_content)

print("Schema fixed in data_loader.py")
