import duckdb
import time
from datetime import datetime, timedelta

print("=== CLOUD DATA WAREHOUSE SIMULATION ===")
print("Using DuckDB as distributed query engine\n")

# Connect to existing database
conn = duckdb.connect('finance_data.db')

# 1. Create sample data for advanced queries
print("1. Creating sample data for warehouse simulation...")

# Create a larger transactions table for distributed query testing
conn.execute("""
    CREATE OR REPLACE TABLE warehouse_transactions AS
    SELECT 
        t.*,
        d.year,
        d.month,
        d.day_of_week,
        c.category_group,
        a.account_type,
        m.merchant_type,
        CASE 
            WHEN t.amount < -1000 THEN 'VERY_LARGE'
            WHEN t.amount < -500 THEN 'LARGE'
            WHEN t.amount < -100 THEN 'MEDIUM'
            WHEN t.amount < 0 THEN 'SMALL'
            ELSE 'INCOME'
        END as amount_category
    FROM fact_transactions t
    LEFT JOIN dim_date d ON t.date_key = d.date_key
    LEFT JOIN dim_category c ON t.category_id = c.category_id
    LEFT JOIN dim_account a ON t.account_id = a.account_id
    LEFT JOIN dim_merchant m ON t.merchant_id = m.merchant_id
""")

row_count = conn.execute("SELECT COUNT(*) FROM warehouse_transactions").fetchone()[0]
print(f"   ✅ Created warehouse_transactions with {row_count} rows")

# 2. WINDOW FUNCTIONS for running totals
print("\n2. Window Functions - Running Totals:")
print("   " + "="*40)

running_totals = conn.execute("""
    WITH daily_totals AS (
        SELECT 
            date_key,
            SUM(amount) as daily_amount,
            COUNT(*) as transaction_count
        FROM warehouse_transactions
        GROUP BY date_key
        ORDER BY date_key
    )
    SELECT
        date_key,
        daily_amount,
        transaction_count,
        SUM(daily_amount) OVER (ORDER BY date_key) as running_balance,
        AVG(daily_amount) OVER (ORDER BY date_key ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) as weekly_moving_avg,
        RANK() OVER (ORDER BY daily_amount DESC) as daily_rank
    FROM daily_totals
    ORDER BY date_key
    LIMIT 10
""").df()

print(running_totals.to_string(index=False))

# 3. RECURSIVE CTEs for category hierarchies
print("\n3. Recursive CTEs - Category Hierarchies:")
print("   " + "="*40)

# Create a category hierarchy table
conn.execute("""
    CREATE OR REPLACE TABLE category_hierarchy (
        category_id INTEGER,
        category_name VARCHAR,
        parent_category_id INTEGER,
        level INTEGER
    )
""")

# Sample hierarchy data
hierarchy_data = [
    (1, 'All', None, 0),
    (2, 'Expenses', 1, 1),
    (3, 'Income', 1, 1),
    (4, 'Necessities', 2, 2),
    (5, 'Discretionary', 2, 2),
    (6, 'Food', 4, 3),
    (7, 'Housing', 4, 3),
    (8, 'Entertainment', 5, 3),
    (9, 'Shopping', 5, 3),
    (10, 'Salary', 3, 2),
    (11, 'Investment', 3, 2)
]

for row in hierarchy_data:
    conn.execute("INSERT INTO category_hierarchy VALUES (?, ?, ?, ?)", row)

# Recursive query to traverse hierarchy
recursive_result = conn.execute("""
    WITH RECURSIVE category_tree AS (
        -- Anchor: top level categories
        SELECT 
            category_id,
            category_name,
            parent_category_id,
            level,
            category_name as full_path
        FROM category_hierarchy
        WHERE parent_category_id IS NULL
        
        UNION ALL
        
        -- Recursive: child categories
        SELECT 
            c.category_id,
            c.category_name,
            c.parent_category_id,
            c.level,
            t.full_path || ' → ' || c.category_name
        FROM category_hierarchy c
        JOIN category_tree t ON c.parent_category_id = t.category_id
    )
    SELECT 
        level,
        REPEAT('  ', level) || category_name as category_tree,
        full_path
    FROM category_tree
    ORDER BY full_path
""").df()

print(recursive_result.to_string(index=False))

# 4. QUERY OPTIMIZATION with EXPLAIN plans
print("\n4. Query Optimization - EXPLAIN Plans:")
print("   " + "="*40)

# Show query plan for complex query
complex_query = """
    SELECT 
        w.year,
        w.month,
        w.category_group,
        w.account_type,
        COUNT(*) as transaction_count,
        SUM(w.amount) as total_amount,
        AVG(w.amount) as avg_amount,
        MIN(w.amount) as min_amount,
        MAX(w.amount) as max_amount
    FROM warehouse_transactions w
    WHERE w.year = 2025
        AND w.amount < 0
        AND w.category_group IN ('Dining', 'Retail', 'Transportation')
    GROUP BY w.year, w.month, w.category_group, w.account_type
    HAVING COUNT(*) > 2
    ORDER BY total_amount
"""

print("Complex Query:")
print("-" * 30)
for line in complex_query.split('\n'):
    print(f"  {line}")

print("\nEXPLAIN Plan:")
print("-" * 30)
explain_result = conn.execute(f"EXPLAIN {complex_query}").fetchall()
for row in explain_result:
    print(row[0])

# 5. MATERIALIZED VIEWS for common queries
print("\n5. Materialized Views - Common Query Optimization:")
print("   " + "="*40)

# Create materialized view (simulated with regular view in DuckDB)
conn.execute("""
    CREATE OR REPLACE VIEW monthly_financial_summary AS
    SELECT
        year,
        month,
        category_group,
        account_type,
        COUNT(*) as transaction_count,
        SUM(amount) as net_amount,
        SUM(CASE WHEN amount < 0 THEN amount ELSE 0 END) as total_expenses,
        SUM(CASE WHEN amount > 0 THEN amount ELSE 0 END) as total_income,
        AVG(amount) as avg_transaction,
        MIN(date_key) as first_date,
        MAX(date_key) as last_date
    FROM warehouse_transactions
    GROUP BY year, month, category_group, account_type
""")

# Query the materialized view
view_query = conn.execute("""
    SELECT 
        year,
        month,
        category_group,
        ROUND(total_expenses, 2) as expenses,
        ROUND(total_income, 2) as income,
        ROUND(net_amount, 2) as net,
        transaction_count
    FROM monthly_financial_summary
    WHERE year = 2025
    ORDER BY year, month, category_group
    LIMIT 15
""").df()

print("Materialized View Query Results:")
print(view_query.to_string(index=False))

# 6. DISTRIBUTED QUERY ENGINE SIMULATION
print("\n6. Distributed Query Engine Features:")
print("   " + "="*40)

# Simulate parallel query execution
print("   Simulating distributed execution...")

queries = [
    "SELECT COUNT(*) FROM warehouse_transactions",
    "SELECT AVG(amount) FROM warehouse_transactions WHERE amount < 0",
    "SELECT COUNT(DISTINCT account_type) FROM warehouse_transactions",
    "SELECT SUM(amount) FROM warehouse_transactions WHERE amount > 0"
]

start_time = time.time()

results = []
for i, query in enumerate(queries, 1):
    result = conn.execute(query).fetchone()[0]
    results.append(result)
    print(f"   Query {i} completed: {result}")

execution_time = time.time() - start_time
print(f"\n   Total execution time: {execution_time:.2f} seconds")
print(f"   Average per query: {execution_time/len(queries):.2f} seconds")

# 7. Performance comparison
print("\n7. Performance Comparison:")
print("   " + "="*40)

# Test with and without optimization
test_query = """
    SELECT 
        year,
        month,
        category_group,
        SUM(amount) as total
    FROM warehouse_transactions
    WHERE year = 2025
        AND month BETWEEN 1 AND 12
        AND amount < 0
    GROUP BY year, month, category_group
    ORDER BY total
"""

# Without optimization
start = time.time()
conn.execute(test_query).fetchall()
no_opt_time = time.time() - start

# With optimization (using materialized view)
start = time.time()
conn.execute("""
    SELECT 
        year,
        month,
        category_group,
        total_expenses as total
    FROM monthly_financial_summary
    WHERE year = 2025
        AND month BETWEEN 1 AND 12
    ORDER BY total
""").fetchall()
opt_time = time.time() - start

print(f"   Without optimization: {no_opt_time:.3f} seconds")
print(f"   With optimization:    {opt_time:.3f} seconds")
print(f"   Speed improvement:    {no_opt_time/opt_time:.1f}x faster")

conn.close()

print("\n" + "="*50)
print("✅ CLOUD DATA WAREHOUSE SIMULATION COMPLETE")
print("="*50)
print("\nTechniques implemented:")
print("  1. ✅ Window functions (running totals, moving averages)")
print("  2. ✅ Recursive CTEs (hierarchy traversal)")
print("  3. ✅ EXPLAIN query plans (optimization analysis)")
print("  4. ✅ Materialized views (common query optimization)")
print("  5. ✅ Distributed query simulation")
print("  6. ✅ Performance benchmarking")
