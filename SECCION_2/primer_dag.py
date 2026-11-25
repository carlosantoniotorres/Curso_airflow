from airflow import DAG  # Importa la clase DAG de Airflow para crear el flujo de trabajo.
from airflow.operators.empty import EmptyOperator  # Importa EmptyOperator, que crea tareas vacías sin lógica.
from datetime import datetime  # Importa datetime para definir la fecha de inicio del DAG.


with DAG(  # Inicia la definición del DAG llamado "primer_dag".
    dag_id="primer_dag",  # Identificador único del DAG en Airflow.
    start_date=datetime(2026, 11, 14),  # Fecha desde la que Airflow empieza a programar las ejecuciones.
    schedule="@daily",  # Ejecuta el DAG una vez al día.
    catchup=False,  # Evita ejecutar ejecuciones pasadas que se hayan perdido.
) as dag:  # Asigna el DAG a la variable dag.
    inicio = EmptyOperator(task_id="inicio")  # Crea la tarea de inicio, vacía, con ID "inicio".
    fin = EmptyOperator(task_id='fin')  # Crea la tarea final, vacía, con ID "fin".
    inicio >> fin  # Define la dependencia: la tarea inicio debe ejecutarse antes que la tarea fin.