from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import requests
import json

default_args = {
    'start_date': datetime(2024, 1, 1),
}

with DAG(
    dag_id='ingesta_api_pokemon',
    default_args=default_args,
    schedule='@daily',
    catchup=False,
    description='Pipeline de ingesta y transformación de datos desde la API de Pokémon',
    tags=['proyecto', 'api', 'ingesta'],
) as dag:
    def obtener_pokemon():
        url = 'https://pokeapi.co/api/v2/pokemon/1'
        response = requests.get(url)
        data = response.json()
        with open('/tmp/pokemon_raw.json', 'w') as f:
            json.dump(data, f)
        print("Datos crudos guardados.")

    ingesta = PythonOperator(
        task_id='obtener_datos_api',
        python_callable=obtener_pokemon
    )

    def transformar_datos():
        with open('/tmp/pokemon_raw.json') as f:
            data = json.load(f)
        nombre = data['name']
        peso = data['weight']
        altura = data['height']
        resumen = {
            'nombre': nombre,
            'peso': peso,
            'altura': altura
        }
        with open('/tmp/pokemon_limpio.json', 'w') as f:
            json.dump(resumen, f)
        print("Datos transformados.")

    transformar = PythonOperator(
        task_id='transformar_datos',
        python_callable=transformar_datos
    )

    def almacenar_datos():
        with open('/tmp/pokemon_limpio.json') as f:
            datos = json.load(f)
        print("Guardado final:", datos)

    guardar = PythonOperator(
        task_id='guardar_datos',
        python_callable=almacenar_datos
    )

    ingesta >> transformar >> guardar
