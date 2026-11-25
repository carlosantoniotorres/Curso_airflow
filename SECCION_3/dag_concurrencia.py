from airflow import dag
from airflow.operators.python import PythonOperator
form datetime import datetime

def tarea():
    print('Ejecutando tarea')

with DAG(
    dag_id="dag_concurrencia",
    start_date="datetime(2026,11,24)",
    schedule='@daily',
    catchup=False,
    max_activate_task=5,
    max_activate_runs=2
) as dag:
    tarea1 = PythonOperator(
        task_id='tarea1',
        python_callable=tarea
        pool='pool2'
    )