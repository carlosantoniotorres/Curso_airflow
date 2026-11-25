from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
import random


def success_taks():
    #Simula una tarea que tiene falla a veces
    if random.random() < 0.5:
        raise Exception('Fallo por que si')
    return "A veces funciona"


def slow_task():
    import time
    time.sleep(random.randint(5,8)) #Simula duracion larga
    return "Tardo pore llego"

