from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

def procesar_ventas():
    print("Procesando archivo de ventas...")  # En un mundo ideal, este DAG esperaría a que DAG A termine

with DAG(
    dag_id="dag_b",
    start_date=datetime(2024, 1, 1),
    schedule="@daily",  # Esto es lo problemático: puede ejecutarse aunque no haya nuevos datos
    catchup=False,
    tags=["ejemplo"]
) as dag:

    procesar = PythonOperator(
        task_id="procesar_ventas",
        python_callable=procesar_ventas
    )
