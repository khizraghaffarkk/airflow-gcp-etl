from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

# Define a simple Python function
def hello_world():
    print("Hello, Airflow!")

# Define the DAG
with DAG(
    dag_id="hello_world_dag",
    start_date=datetime(2025, 10, 6),
    schedule_interval="@daily",
    catchup=False,
    tags=["example"],
) as dag:

    # Define a task
    task1 = PythonOperator(
        task_id="print_hello",
        python_callable=hello_world
    )

    # Set task order (only one task here)
    task1
