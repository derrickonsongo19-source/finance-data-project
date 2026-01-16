"""
Monthly Financial Reporting DAG
Generates comprehensive monthly reports and analytics
"""
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
import sys
import os
import polars as pl
import duckdb

sys.path.insert(0, '/home/derrick-onsongo/finance-data-project')

default_args = {
    'owner': 'finance_team',
    'depends_on_past': False,
    'start_date': datetime(2025, 1, 1),
    'email': ['admin@finance.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=10),
}

dag = DAG(
    'finance_monthly_reporting',
    default_args=default_args,
    description='Monthly financial reports and analytics',
    schedule='0 3 1 * *',  # Run at 3 AM on the 1st of every month
    catchup=False,
    tags=['finance', 'monthly', 'reporting'],
)

def generate_monthly_summary():
    """Generate comprehensive monthly financial summary"""
    conn = duckdb.connect('/home/derrick-onsongo/finance-data-project/finance_data.db')
    
    # Monthly summary - convert from pandas to polars
    monthly_summary_pd = conn.execute("""
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
        WHERE year = EXTRACT(YEAR FROM CURRENT_DATE - INTERVAL 1 MONTH)
          AND month = EXTRACT(MONTH FROM CURRENT_DATE - INTERVAL 1 MONTH)
        GROUP BY year, month, category_group, account_type
        ORDER BY total_expenses DESC
    """).df()
    
    monthly_summary = pl.from_pandas(monthly_summary_pd)
    
    # Save monthly summary
    os.makedirs('/home/derrick-onsongo/finance-data-project/reports/monthly', exist_ok=True)
    report_path = f'/home/derrick-onsongo/finance-data-project/reports/monthly/summary_{datetime.now().strftime("%Y%m")}.csv'
    monthly_summary.write_csv(report_path)
    print(f"📈 Monthly summary saved to: {report_path}")
    
    # Generate insights
    insights_pd = conn.execute("""
        WITH monthly_data AS (
            SELECT
                category_group,
                SUM(CASE WHEN amount < 0 THEN amount ELSE 0 END) as monthly_expenses
            FROM warehouse_transactions
            WHERE year = EXTRACT(YEAR FROM CURRENT_DATE - INTERVAL 1 MONTH)
              AND month = EXTRACT(MONTH FROM CURRENT_DATE - INTERVAL 1 MONTH)
            GROUP BY category_group
        )
        SELECT
            category_group,
            monthly_expenses,
            ROUND(monthly_expenses * 100.0 / SUM(monthly_expenses) OVER(), 2) as percentage
        FROM monthly_data
        ORDER BY monthly_expenses
    """).df()
    
    insights = pl.from_pandas(insights_pd)
    
    insights_path = f'/home/derrick-onsongo/finance-data-project/reports/monthly/insights_{datetime.now().strftime("%Y%m")}.csv'
    insights.write_csv(insights_path)
    print(f"🔍 Monthly insights saved to: {insights_path}")
    
    conn.close()
    return True

def generate_budget_vs_actual():
    """Compare actual spending vs budget"""
    conn = duckdb.connect('/home/derrick-onsongo/finance-data-project/finance_data.db')
    
    # Assuming you have a budget table - create mock if not exists
    conn.execute("""
        CREATE TABLE IF NOT EXISTS monthly_budget (
            year INTEGER,
            month INTEGER,
            category_group VARCHAR,
            budget_amount DECIMAL(10,2)
        )
    """)
    
    # Compare actual vs budget
    comparison_pd = conn.execute("""
        SELECT
            COALESCE(b.category_group, a.category_group) as category_group,
            COALESCE(b.budget_amount, 0) as budgeted,
            COALESCE(SUM(CASE WHEN a.amount < 0 THEN a.amount ELSE 0 END), 0) as actual_expenses,
            COALESCE(b.budget_amount, 0) + COALESCE(SUM(CASE WHEN a.amount < 0 THEN a.amount ELSE 0 END), 0) as variance
        FROM monthly_budget b
        FULL OUTER JOIN warehouse_transactions a 
            ON b.category_group = a.category_group
            AND b.year = EXTRACT(YEAR FROM CURRENT_DATE - INTERVAL 1 MONTH)
            AND b.month = EXTRACT(MONTH FROM CURRENT_DATE - INTERVAL 1 MONTH)
            AND a.year = EXTRACT(YEAR FROM CURRENT_DATE - INTERVAL 1 MONTH)
            AND a.month = EXTRACT(MONTH FROM CURRENT_DATE - INTERVAL 1 MONTH)
        GROUP BY b.category_group, b.budget_amount, a.category_group
    """).df()
    
    comparison = pl.from_pandas(comparison_pd)
    
    comparison_path = f'/home/derrick-onsongo/finance-data-project/reports/monthly/budget_vs_actual_{datetime.now().strftime("%Y%m")}.csv'
    comparison.write_csv(comparison_path)
    print(f"💰 Budget vs Actual saved to: {comparison_path}")
    
    conn.close()
    return True

def send_monthly_report():
    """Simulate sending monthly report (email, slack, etc.)"""
    report_dir = '/home/derrick-onsongo/finance-data-project/reports/monthly/'
    files = os.listdir(report_dir) if os.path.exists(report_dir) else []
    
    print(f"📧 Monthly report ready. Files generated: {len(files)}")
    for file in files[-3:]:  # Show last 3 files
        print(f"   - {file}")
    
    # In production, add email/Slack notification here
    return True

# Define tasks
start = EmptyOperator(task_id='start', dag=dag)

monthly_summary_task = PythonOperator(
    task_id='generate_monthly_summary',
    python_callable=generate_monthly_summary,
    dag=dag
)

budget_analysis_task = PythonOperator(
    task_id='budget_vs_actual_analysis',
    python_callable=generate_budget_vs_actual,
    dag=dag
)

send_report_task = PythonOperator(
    task_id='send_monthly_report',
    python_callable=send_monthly_report,
    dag=dag
)

success_notification = BashOperator(
    task_id='success_notification',
    bash_command='echo "📊 Monthly financial reporting completed successfully!"',
    dag=dag
)

# Task dependencies
start >> monthly_summary_task >> budget_analysis_task >> send_report_task >> success_notification

# Documentation
dag.doc_md = """
# Monthly Financial Reporting DAG

Generates comprehensive monthly financial reports including:
1. Monthly transaction summary
2. Spending insights and trends
3. Budget vs Actual analysis
4. Report distribution

## Schedule:
- Runs on the 1st of every month at 3:00 AM
- Processes data from the previous month
"""

monthly_summary_task.doc_md = "Generates summary of all transactions for the previous month by category and account type."
budget_analysis_task.doc_md = "Compares actual spending against budget targets for each category."
send_report_task.doc_md = "Prepares and sends monthly report (simulated - add email/Slack in production)."
