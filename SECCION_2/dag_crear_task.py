from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime


def saludar():
    print("Hola, futuro maestro de Airflow!")
with DAG(
    dag_id='dag_crear_task',
    start_date=datetime(2026, 11, 18),
    schedule='@daily',
    catchup=False,
    tags=['curso'],
) as dag:
    
    tarea_saludo = PythonOperator(
        task_id='saludar',
        python_callable=saludar
        )