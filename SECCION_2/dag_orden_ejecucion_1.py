from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime


def extraer():
    print('Extrayendo datos...')
def transformar():
    print('transformando datos...')
def cargar():
    print('Cargando datos...')
def terminar():
    print('Terminando...')
with DAG(
    dag_id='dag_orden_ejecucion_1',
    start_date =datetime(2026, 11, 18),
    schedule="@daily",
    catchup=False,
    tags=["ejemplos", "orden_ejecucion"]
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
    t4 = PythonOperator(
        task_id="terminar",
        python_callable=terminar
    )
    
    #IMportante: No hay dependencias entre las tareas t1 >> t2 >> t3
    
    t1 >> t2 >> t3 >> t4
    # t1.set_downstream(t2)
    # t2.set_upstream(t1)

    # t1 >> [t2, t3] >> t4