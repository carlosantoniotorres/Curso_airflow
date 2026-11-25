from airflow.sdk import DAG, Asset
from airflow.providers.standard.operators.python import PythonOperator
from datetime import datetime


dataset_path = "/tmp/datos_frescos.csv"
dataset = Asset(dataset_path)


def generar_csv():
    with open(dataset_path, "w") as f:
        f.write("fecha,valor\n2025-01-01,100")


with DAG(
    dag_id="dag_productor",
    start_date=datetime(2025, 1, 1),
    schedule="@daily",
    catchup=False
) as dag:

    tarea = PythonOperator(
        task_id="generar_csv",
        python_callable=generar_csv,
        outlets=[dataset]
    )
