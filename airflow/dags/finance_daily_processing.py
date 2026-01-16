from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator  # CHANGED from dummy
import sys
import os

# Add project path to import your scripts
sys.path.insert(0, '/home/derrick-onsongo/finance-data-project')

default_args = {
    'owner': 'finance_team',
    'depends_on_past': False,
    'start_date': datetime(2025, 1, 1),
    'email': ['admin@finance.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define the DAG
dag = DAG(
    'finance_daily_processing',
    default_args=default_args,
    description='Daily financial data processing pipeline',
    schedule='0 2 * * *',  # CHANGED from schedule_interval, Run daily at 2 AM
    catchup=False,
    tags=['finance', 'batch', 'etl'],
)

def run_bronze_layer():
    """Execute bronze layer data ingestion"""
    import subprocess
    result = subprocess.run(
        ['python3', '/home/derrick-onsongo/finance-data-project/bronze_layer.py'],
        capture_output=True,
        text=True
    )
    if result.returncode != 0:
        raise Exception(f"Bronze layer failed: {result.stderr}")
    print("✅ Bronze layer completed")

def run_silver_layer():
    """Execute silver layer data cleaning"""
    import subprocess
    result = subprocess.run(
        ['python3', '/home/derrick-onsongo/finance-data-project/silver_layer.py'],
        capture_output=True,
        text=True
    )
    if result.returncode != 0:
        raise Exception(f"Silver layer failed: {result.stderr}")
    print("✅ Silver layer completed")

def run_gold_layer():
    """Execute gold layer aggregation"""
    import subprocess
    result = subprocess.run(
        ['python3', '/home/derrick-onsongo/finance-data-project/gold_layer.py'],
        capture_output=True,
        text=True
    )
    if result.returncode != 0:
        raise Exception(f"Gold layer failed: {result.stderr}")
    print("✅ Gold layer completed")

def run_dbt_transformations():
    """Execute dbt transformations"""
    import subprocess
    result = subprocess.run(
        ['cd', '/home/derrick-onsongo/finance-data-project/finance_dbt', '&&', 'dbt', 'run'],
        shell=True,
        capture_output=True,
        text=True
    )
    if result.returncode != 0:
        raise Exception(f"dbt transformations failed: {result.stderr}")
    print("✅ dbt transformations completed")

def data_quality_check():
    """Run data quality tests"""
    import subprocess
    result = subprocess.run(
        ['cd', '/home/derrick-onsongo/finance-data-project/finance_dbt', '&&', 'dbt', 'test'],
        shell=True,
        capture_output=True,
        text=True
    )
    if result.returncode != 0:
        raise Exception(f"Data quality tests failed: {result.stderr}")
    print("✅ Data quality tests passed")

def generate_daily_report():
    """Generate daily financial report"""
    import duckdb
    
    conn = duckdb.connect('/home/derrick-onsongo/finance-data-project/finance_data.db')
    
    # Generate report
    report = conn.execute("""
        SELECT 
            date_key as date,
            COUNT(*) as transaction_count,
            SUM(amount) as net_amount,
            SUM(CASE WHEN amount < 0 THEN amount ELSE 0 END) as total_expenses,
            SUM(CASE WHEN amount > 0 THEN amount ELSE 0 END) as total_income
        FROM warehouse_transactions
        WHERE date_key = CURRENT_DATE - INTERVAL 1 DAY
        GROUP BY date_key
    """).df()
    
    # Save report
    report_path = '/home/derrick-onsongo/finance-data-project/reports/daily_report.csv'
    os.makedirs(os.path.dirname(report_path), exist_ok=True)
    report.to_csv(report_path, index=False)
    
    print(f"📊 Daily report saved to {report_path}")
    print(report)
    
    conn.close()

# Define tasks
start = EmptyOperator(task_id='start', dag=dag)  # CHANGED from DummyOperator

bronze_task = PythonOperator(
    task_id='bronze_layer_ingestion',
    python_callable=run_bronze_layer,
    dag=dag
)

silver_task = PythonOperator(
    task_id='silver_layer_cleaning',
    python_callable=run_silver_layer,
    dag=dag
)

gold_task = PythonOperator(
    task_id='gold_layer_aggregation',
    python_callable=run_gold_layer,
    dag=dag
)

dbt_task = PythonOperator(
    task_id='dbt_transformations',
    python_callable=run_dbt_transformations,
    dag=dag
)

quality_task = PythonOperator(
    task_id='data_quality_checks',
    python_callable=data_quality_check,
    dag=dag
)

report_task = PythonOperator(
    task_id='generate_daily_report',
    python_callable=generate_daily_report,
    dag=dag
)

success_notification = BashOperator(
    task_id='success_notification',
    bash_command='echo "✅ Daily finance processing completed successfully!"',
    dag=dag
)

# Define task dependencies
start >> bronze_task >> silver_task >> gold_task >> dbt_task >> quality_task >> report_task >> success_notification

# Add documentation
dag.doc_md = """
# Daily Financial Data Processing Pipeline

This DAG orchestrates the daily batch processing of financial data.

## Pipeline Steps:
1. **Bronze Layer**: Raw data ingestion from sources
2. **Silver Layer**: Data cleaning and transformation
3. **Gold Layer**: Business-level aggregations
4. **dbt Transformations**: SQL-based transformations
5. **Data Quality Checks**: Validate data integrity
6. **Daily Report Generation**: Create summary reports

## Schedule:
- Runs daily at 2:00 AM UTC
- Processes data from the previous day
"""

# Task documentation
bronze_task.doc_md = "Ingest raw data from PostgreSQL, CSV files, and APIs into bronze layer (Parquet format)."
silver_task.doc_md = "Clean and transform raw data using Polars, remove duplicates, handle nulls."
gold_task.doc_md = "Create aggregated business-ready tables for analytics and reporting."
quality_task.doc_md = "Run dbt data quality tests to ensure data integrity."
