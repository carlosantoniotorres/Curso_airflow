from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

def generar_datos_ventas():
    print("Generando archivo de ventas...")  # Aquí podrías escribir un CSV, parquet, etc.

with DAG(
    dag_id="dag_a",
    start_date=datetime(2024, 1, 1),
    schedule="@daily",
    catchup=False,
    tags=["ejemplo"]
) as dag:

    generar_datos = PythonOperator(
        task_id="generar_datos_ventas",
        python_callable=generar_datos_ventas
    )
