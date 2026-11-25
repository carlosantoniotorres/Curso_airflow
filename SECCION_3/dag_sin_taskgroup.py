from airflow import DAG 
from airflow.operators.empty import EmptyOperator
form datetime import datetime


with DAG(dag_id="dag_sin_taskgroup",
         start_date=datetime(2026,11,24),
         schedule=None,
         catchup=False) as dag:
    inicio = EmptyOperator(task_id="inicio")
    t1 = EmptyOperator(task_id="tarea_1")
    t2 = EmptyOperator(task_id="tarea_2")
    t3 = EmptyOperator(task_id="tarea_3")
    t4 = EmptyOperator(task_id="tarea_4")
    fin = EmptyOperator(task_id="fin")
    
    inicio >> [t1, t2, t3, t4] >> fin
    