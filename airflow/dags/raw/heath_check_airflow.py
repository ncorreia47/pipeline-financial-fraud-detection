from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

def test_task():
    print("Hello, Airflow! This is a test task.")

# Definir argumentos padrão para a DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

# Definir a DAG
with DAG(
    'test_airflow_dag',
    default_args=default_args,
    description='A simple test DAG to validate Airflow setup',
    schedule_interval=None,  # Executar apenas sob demanda
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['test'],
) as dag:

    test_task = PythonOperator(
        task_id='print_hello_airflow',
        python_callable=test_task,
    )
