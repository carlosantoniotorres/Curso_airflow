from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import sys
sys.path.append("/home/ServerCar/airflow/dags/proyecto_ingesta")  # Ajusta si es necesario
from datasets import clean_dataset

def cargar():
    with open("/tmp/clean_data.json") as f:
        contenido = f.read()
    print("Cargando a BBDD... (simulado)")
    print(contenido)

with DAG(
    dag_id="dag_carga",
    schedule=[clean_dataset],
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["ETL"],
    description="Carga los datos limpios a destino final",
    default_args={"owner": "airflow"},
) as dag:

    cargar_datos = PythonOperator(
        task_id="cargar_datos",
        python_callable=cargar,
    )
