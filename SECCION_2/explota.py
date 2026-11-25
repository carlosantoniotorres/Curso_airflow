from airflow import DAG  # Importa la clase que permite definir un flujo de trabajo de Airflow.
from airflow.operators.python import PythonOperator  # Importa el operador que ejecuta una función de Python.
from datetime import datetime  # Importa datetime para indicar la fecha de inicio del DAG.

def explota():  # Define la función que se ejecutará como parte de una tarea.
    return 1 / 0  # Intenta dividir entre cero y, al ejecutarse, genera un ZeroDivisionError.

with DAG(  # Crea y configura el DAG; las tareas indentadas pertenecerán a este flujo.
    dag_id="dag_con_error",  # Establece el identificador único del DAG en Airflow.
    start_date=datetime(2023, 1, 1),  # Define la fecha desde la que el DAG puede ejecutarse.
    schedule=None,  # No programa ejecuciones automáticas; el DAG debe iniciarse manualmente o por API.
    catchup=False,  # Evita que Airflow cree ejecuciones atrasadas para intervalos anteriores.
) as dag:  # Guarda el DAG en la variable dag y activa el contexto para definir sus tareas.
    fallo = PythonOperator(task_id="task_falla", python_callable=explota)  # Crea la tarea que llama a explota; fallará al intentar dividir entre cero.
