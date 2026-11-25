from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import sys
sys.path.append("/home/ServerCar/airflow/dags/proyecto_ingesta")  # Ajusta si es necesario
from datasets import raw_dataset

def extraer_datos():
    with open("/tmp/raw_data.json", "w") as f:
        f.write('{"nombre": "Ada Lovelace", "edad": 36}')

with DAG(
    dag_id="dag_ingesta",
    schedule="@daily",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["ETL"],
    description="DAG que ingesta datos crudos",
    default_args={"owner": "airflow"}    
) as dag:

    ingestar = PythonOperator(
        task_id="ingestar_datos",
        python_callable=extraer_datos,
        outlets=[raw_dataset]
    )
