from datetime import datetime
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

def print_hello():
    print("✅ Hello from Airflow!")
    return "Hello World"

with DAG(
    dag_id="test_finance_dag",
    start_date=datetime(2025, 1, 1),
    schedule="@daily",  # CHANGED from schedule_interval
    catchup=False,
    tags=["test"],
) as dag:
    
    task1 = BashOperator(
        task_id="print_date",
        bash_command="date",
    )
    
    task2 = PythonOperator(
        task_id="print_hello",
        python_callable=print_hello,
    )
    
    task3 = BashOperator(
        task_id="echo_done",
        bash_command='echo "Test DAG completed successfully!"',
    )
    
    task1 >> task2 >> task3
