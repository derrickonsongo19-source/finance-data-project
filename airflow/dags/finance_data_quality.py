"""
Data Quality Monitoring DAG
Runs comprehensive data quality checks on financial data
"""
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
import sys
import os
import pandas as pd
import duckdb

sys.path.insert(0, '/home/derrick-onsongo/finance-data-project')

default_args = {
    'owner': 'data_quality_team',
    'depends_on_past': False,
    'start_date': datetime(2025, 1, 1),
    'email': ['admin@finance.com'],
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'finance_data_quality',
    default_args=default_args,
    description='Comprehensive data quality monitoring and validation',
    schedule='30 1 * * *',  # Run daily at 1:30 AM (after daily processing)
    catchup=False,
    tags=['finance', 'data-quality', 'validation'],
)

def check_data_completeness():
    """Check for missing data and null values"""
    conn = duckdb.connect('/home/derrick-onsongo/finance-data-project/finance_data.db')
    
    completeness_checks = [
        ("Total transaction count", "SELECT COUNT(*) FROM warehouse_transactions"),
        ("Transactions today", "SELECT COUNT(*) FROM warehouse_transactions WHERE date_key = CURRENT_DATE"),
        ("Null amount check", "SELECT COUNT(*) FROM warehouse_transactions WHERE amount IS NULL"),
        ("Null date check", "SELECT COUNT(*) FROM warehouse_transactions WHERE date_key IS NULL"),
        ("Null category check", "SELECT COUNT(*) FROM warehouse_transactions WHERE category IS NULL"),
    ]
    
    print("📊 Data Completeness Checks:")
    print("=" * 40)
    
    results = []
    for check_name, query in completeness_checks:
        count = conn.execute(query).fetchone()[0]
        results.append((check_name, count))
        status = "✅" if count == 0 or check_name == "Total transaction count" else "❌"
        print(f"{status} {check_name}: {count}")
    
    # Fail if critical nulls found
    null_checks = [r for r in results if "Null" in r[0] and r[1] > 0]
    if null_checks:
        raise Exception(f"Data completeness check failed: {null_checks}")
    
    conn.close()
    return True

def check_data_accuracy():
    """Validate data accuracy and business rules"""
    conn = duckdb.connect('/home/derrick-onsongo/finance-data-project/finance_data.db')
    
    accuracy_checks = [
        ("Negative balance accounts", 
         "SELECT COUNT(*) FROM warehouse_transactions WHERE account_balance < 0"),
        ("Future dated transactions", 
         "SELECT COUNT(*) FROM warehouse_transactions WHERE date_key > CURRENT_DATE"),
        ("Zero amount transactions", 
         "SELECT COUNT(*) FROM warehouse_transactions WHERE amount = 0"),
        ("Duplicate transactions (same amount, date, merchant)", 
         """SELECT COUNT(*) FROM (
            SELECT transaction_id, amount, date_key, merchant, COUNT(*)
            FROM warehouse_transactions
            GROUP BY transaction_id, amount, date_key, merchant
            HAVING COUNT(*) > 1
         ) dupes"""),
    ]
    
    print("\n🎯 Data Accuracy Checks:")
    print("=" * 40)
    
    for check_name, query in accuracy_checks:
        count = conn.execute(query).fetchone()[0]
        status = "✅" if count == 0 else "⚠️"
        print(f"{status} {check_name}: {count}")
    
    conn.close()
    return True

def check_data_freshness():
    """Ensure data is up-to-date"""
    conn = duckdb.connect('/home/derrick-onsongo/finance-data-project/finance_data.db')
    
    freshness_query = """
        SELECT 
            MAX(date_key) as latest_date,
            CURRENT_DATE as today,
            DATEDIFF('day', MAX(date_key), CURRENT_DATE) as days_behind
        FROM warehouse_transactions
    """
    
    result = conn.execute(freshness_query).fetchone()
    latest_date, today, days_behind = result
    
    print("\n🕒 Data Freshness Check:")
    print("=" * 40)
    print(f"Latest transaction date: {latest_date}")
    print(f"Today's date: {today}")
    print(f"Days behind: {days_behind}")
    
    if days_behind > 2:
        raise Exception(f"Data is stale! {days_behind} days behind current date")
    elif days_behind > 0:
        print("⚠️  Data is slightly behind but acceptable")
    else:
        print("✅ Data is up-to-date")
    
    conn.close()
    return True

def validate_schema():
    """Validate table schema matches expectations"""
    conn = duckdb.connect('/home/derrick-onsongo/finance-data-project/finance_data.db')
    
    expected_schema = {
        'transaction_id': 'VARCHAR',
        'date_key': 'DATE',
        'amount': 'DECIMAL(10,2)',
        'category': 'VARCHAR',
        'merchant': 'VARCHAR',
        'account_id': 'VARCHAR',
        'account_type': 'VARCHAR',
        'account_balance': 'DECIMAL(10,2)',
        'year': 'INTEGER',
        'month': 'INTEGER',
        'day': 'INTEGER',
        'category_group': 'VARCHAR'
    }
    
    actual_schema = conn.execute("DESCRIBE warehouse_transactions").df()
    
    print("\n🔍 Schema Validation:")
    print("=" * 40)
    
    all_valid = True
    for _, row in actual_schema.iterrows():
        col_name = row['column_name']
        col_type = row['column_type']
        expected_type = expected_schema.get(col_name)
        
        if expected_type and expected_type in col_type:
            print(f"✅ {col_name}: {col_type} (matches {expected_type})")
        elif col_name in expected_schema:
            print(f"❌ {col_name}: {col_type} (expected {expected_schema[col_name]})")
            all_valid = False
        else:
            print(f"⚠️  {col_name}: {col_type} (not in expected schema)")
    
    if not all_valid:
        raise Exception("Schema validation failed!")
    
    conn.close()
    return True

def generate_quality_report():
    """Generate comprehensive data quality report"""
    conn = duckdb.connect('/home/derrick-onsongo/finance-data-project/finance_data.db')
    
    quality_metrics = conn.execute("""
        SELECT
            'Completeness' as metric_type,
            ROUND(COUNT(*) * 100.0 / NULLIF(MAX(total_count), 0), 2) as score,
            'Percentage of non-null values' as description
        FROM warehouse_transactions
        CROSS JOIN (SELECT COUNT(*) as total_count FROM warehouse_transactions) totals
        WHERE amount IS NOT NULL 
          AND date_key IS NOT NULL 
          AND category IS NOT NULL
        UNION ALL
        SELECT
            'Accuracy' as metric_type,
            ROUND((1.0 - COUNT(*) * 1.0 / NULLIF(MAX(total_count), 0)) * 100, 2) as score,
            'Percentage of valid transactions' as description
        FROM warehouse_transactions
        CROSS JOIN (SELECT COUNT(*) as total_count FROM warehouse_transactions) totals
        WHERE amount != 0 AND date_key <= CURRENT_DATE
        UNION ALL
        SELECT
            'Consistency' as metric_type,
            ROUND(COUNT(DISTINCT account_type) * 100.0 / NULLIF(MAX(total_accounts), 0), 2) as score,
            'Account type consistency' as description
        FROM warehouse_transactions
        CROSS JOIN (SELECT COUNT(DISTINCT account_id) as total_accounts FROM warehouse_transactions) accounts
    """).df()
    
    os.makedirs('/home/derrick-onsongo/finance-data-project/reports/quality', exist_ok=True)
    report_path = f'/home/derrick-onsongo/finance-data-project/reports/quality/quality_report_{datetime.now().strftime("%Y%m%d")}.csv'
    quality_metrics.to_csv(report_path, index=False)
    
    print(f"\n📈 Quality report saved to: {report_path}")
    print(quality_metrics)
    
    conn.close()
    return True

# Define tasks
start = EmptyOperator(task_id='start', dag=dag)

completeness_task = PythonOperator(
    task_id='check_data_completeness',
    python_callable=check_data_completeness,
    dag=dag
)

accuracy_task = PythonOperator(
    task_id='check_data_accuracy',
    python_callable=check_data_accuracy,
    dag=dag
)

freshness_task = PythonOperator(
    task_id='check_data_freshness',
    python_callable=check_data_freshness,
    dag=dag
)

schema_task = PythonOperator(
    task_id='validate_schema',
    python_callable=validate_schema,
    dag=dag
)

report_task = PythonOperator(
    task_id='generate_quality_report',
    python_callable=generate_quality_report,
    dag=dag
)

success_notification = BashOperator(
    task_id='success_notification',
    bash_command='echo "✅ Data quality checks completed successfully!"',
    dag=dag
)

# Task dependencies (parallel then sequential)
start >> [completeness_task, accuracy_task, freshness_task, schema_task] >> report_task >> success_notification

# Documentation
dag.doc_md = """
# Data Quality Monitoring DAG

Comprehensive data quality checks for financial data:

## Checks Performed:
1. **Completeness**: Missing values and null checks
2. **Accuracy**: Business rule validation
3. **Freshness**: Data timeliness
4. **Schema**: Structure validation
5. **Reporting**: Quality metrics generation

## Schedule:
- Runs daily at 1:30 AM (after daily processing completes)
- Alerting on failures
- Quality reports generated
"""

completeness_task.doc_md = "Checks for missing data, null values, and completeness of all required fields."
accuracy_task.doc_md = "Validates business rules: no negative balances, no future dates, no duplicates."
freshness_task.doc_md = "Ensures data is up-to-date (not more than 2 days behind current date)."
schema_task.doc_md = "Validates table schema matches expected structure and data types."
report_task.doc_md = "Generates comprehensive data quality report with metrics and scores."
