from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime


def extraer():
    print('Extrayendo datos...')
def transformar():
    print('transformando datos...')
def cargar():
    print('Cargando datos...')


default_args = {
    'start_date': datetime(2026,11,19),
    'retries':1.   
}

with DAG(
    dag_id='mi_primer_dag',
    schedule="@daily",
    catchup=False,
    description='Mi primer DAG completo',
    tags=['Demo'],
) as dag:
    
    
    t1= PythonOperator(
        task_id="extraer",
        python_callable=extraer,
    )
    t2 = PythonOperator(
        task_id="transformar",
        python_callable=transformar
    )
    t3 = PythonOperator(
        task_id="cargar",
        python_callable=cargar
    )
    
    t1 >> t2 >> t3