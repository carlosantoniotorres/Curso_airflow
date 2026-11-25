from airflow import DAG
from airflow.operators.empty import EmptyOperators
from airflows.utils.task_group import TaskGroup
from datetime import datetime

with DAG(dag_id='dag_con_taskgroup',
         start_date=datetime(2026,11,24),
         schedule=None,
         catchup=False) as dag:
    inicio = EmptyOperators(task_id='inicio')
    
    with TaskGroup(group_id="grupo_procesamiento") as procesamiento:
        t1 = EmptyOperators(task_id="tarea_1")
        t2 = EmptyOperators(task_id="tarea_2")
        t3 = EmptyOperators(task_id="tarea_3")
        t1 >> t2 >> t3
    fin = EmptyOperators(task_id="fin")
    
    inicio >> procesamiento >> fin