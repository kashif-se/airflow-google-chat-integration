from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta

from google_chat_callbacks import task_fail_alert, task_success_alert

default_args = {
    'start_date': datetime(2023, 6, 1),
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'sample_dag',
    schedule_interval='0 0 * * *',
    default_args=default_args,
    catchup=False
)


def your_task1_function():
    # Sample code for task1
    print("Executing task1...")
    # Your actual task logic here


task1 = PythonOperator(
    task_id='task1',
    python_callable=your_task1_function,
    on_success_callback=task_success_alert,
    on_failure_callback=task_fail_alert,
    dag=dag
)


def your_task2_function():
    # Sample code for task2
    print("Executing task2...")
    # Your actual task logic here


task2 = PythonOperator(
    task_id='task2',
    python_callable=your_task2_function,
    on_success_callback=task_success_alert,
    on_failure_callback=task_fail_alert,
    dag=dag
)


def your_task3_function():
    # Sample code for task3
    print("Executing task3...")
    # Your actual task logic here


task3 = PythonOperator(
    task_id='task3',
    python_callable=your_task3_function,
    on_success_callback=task_success_alert,
    on_failure_callback=task_fail_alert,
    dag=dag
)

task1 >> task2 >> task3
