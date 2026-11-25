from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.utils.trigger_rule import TriggerRule

with DAG(
    dag_id="dag_trigger_cleanup",
    start_date=None,
    schedule=None,
    catchup=False,
) as dag:

    proceso = BashOperator(
        task_id="proceso",
        bash_command="exit 1",  # Simula un fallo
    )

    limpiar = BashOperator(
        task_id="limpiar",
        bash_command="echo 'Limpiando recursos...'",
        trigger_rule=TriggerRule.ALL_DONE,
    )

    proceso >> limpiar
