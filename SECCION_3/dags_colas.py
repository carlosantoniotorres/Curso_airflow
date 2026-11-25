from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

def tarea():
    print("Ejecutando tarea")
    
with DAG('dags_colas', start_date=datetime(2026, 11, 24), schedule='@daily', catchup=False) as dag:
    tarea1 = PythonOperator(
        task_id='tarea_alta_prioridad',
        python_callable=tarea,
        queue='alta_prioridad'
    )
    tarea1 = PythonOperator(
            task_id='tarea_baja_prioridad',
            python_callable=tarea,
            queue='baja_prioridad'
        )
