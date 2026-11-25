from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.trigger_rule import TriggerRule

def exito():
    print("Todo salió bien.")

def fracaso():
    raise Exception("Algo salió mal")

def despues():
    print("Ejecutando sin importar el caos anterior.")

with DAG(
    dag_id="demo_trigger_rules",
    start_date=None,
    schedule=None,
    catchup=False,
) as dag:
    
    tarea_ok = PythonOperator(
        task_id='tarea_ok',
        python_callable=exito,
    )

    tarea_ko = PythonOperator(
        task_id='tarea_ko',
        python_callable=fracaso,
    )

    tarea_final = PythonOperator(
        task_id='tarea_final',
        python_callable=despues,
        trigger_rule=TriggerRule.ALL_DONE,  # ¡Aquí está la magia!
    )

    [tarea_ok, tarea_ko] >> tarea_final
