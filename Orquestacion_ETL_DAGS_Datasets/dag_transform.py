from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import sys
sys.path.append("/home/ServerCar/airflow/dags/proyecto_ingesta")  # Ajusta si es necesario
from datasets import raw_dataset, clean_dataset

def transformar():
    import json
    with open("/tmp/raw_data.json") as f:
        datos = json.load(f)
    datos["edad"] = datos["edad"] + 1  # Ejemplo tonto
    with open("/tmp/clean_data.json", "w") as f:
        json.dump(datos, f)

with DAG(
    dag_id="dag_transform",
    schedule=[raw_dataset],
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["ETL"],
    description="Transforma los datos crudos en limpios",
    default_args={"owner": "airflow"},
) as dag:

    transformar_datos = PythonOperator(
        task_id="transformar_datos",
        python_callable=transformar,
        outlets=[clean_dataset]
    )
