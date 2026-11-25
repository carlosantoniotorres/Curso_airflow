# Curso de Apache Airflow

Repositorio con los ejercicios y ejemplos que realicé durante un curso de **Apache Airflow**, herramienta para diseñar, programar y monitorizar flujos de trabajo. El material combina conceptos básicos con prácticas de DAGs, pipelines ETL, dependencias, datasets, reglas de ejecución y distribución de tareas.

## ¿Qué encontrarás en el curso?

- Fundamentos de Airflow: DAGs, tareas, dependencias y programación de ejecuciones.
- Ejemplos de pipelines de extracción, transformación y carga (ETL).
- Uso de operadores de Python y tareas vacías.
- Agrupación y ejecución condicional de tareas.
- Ejemplos de comunicación entre DAGs mediante datasets y de asignación de tareas a colas.
- Casos de éxito, fallos y limpieza posterior a la ejecución.

## Estructura del repositorio

### `SECCION_2/` — Primeros pasos con DAGs

Ejercicios introductorios para crear DAGs, definir tareas y establecer su orden:

- `primer_dag.py`: DAG sencillo con tareas de inicio y fin.
- `dag_crear_task.py`: tarea de Python que imprime un saludo.
- `mi_primer_dag.py`: ejemplo de flujo ETL lineal con extracción, transformación y carga simuladas.
- `dag_orden_ejecucion_1.py`: varias tareas conectadas en secuencia.
- `explota.py`: ejemplo de tarea que falla intencionalmente para observar el estado de error.
- `ramdon-fail.py`: funciones de ejemplo para simular una tarea con fallos aleatorios y otra de larga duración.

### `SECCION_3/` — Flujos y ejecución avanzada

Ejercicios sobre organización, coordinación y ejecución de tareas:

- `dag_sin_taskgroup.py` y `dag_con_taskgroup.py`: comparación entre tareas relacionadas sin agrupar y el uso de `TaskGroup`.
- `dag_concurrencia.py`: ejemplo relacionado con concurrencia y pools.
- `dags_colas.py`: asignación de tareas a colas.
- `dag_trigger_rules.py` y `dag_trigger_cleanup.py`: reglas de ejecución y limpieza aunque una tarea anterior haya terminado con error.
- `dag_a.py` y `dag_b.py`: ejemplos separados de generación y procesamiento de datos para explorar la coordinación entre DAGs.
- `dag_productor.py` y `dag_consumidor.py`: ejercicios relacionados con datasets y productores/consumidores. Actualmente, `dag_consumidor.py` contiene el mismo ejemplo productor que `dag_productor.py`, por lo que queda pendiente convertirlo en un consumidor.

### `Orquestacion_ETL_DAGS_Datasets/` — Pipeline ETL con datasets

Ejemplo de pipeline separado en DAGs que representan las fases de ingesta, transformación y carga:

- `dag_ingesta.py`: escribe datos crudos y publica el dataset de origen.
- `dag_transform.py`: transforma esos datos y publica el dataset limpio.
- `dag_carga.py`: lee los datos limpios y simula su carga al destino.
- `datasets.py`: define los datasets que conectan las fases del pipeline.

### `Orquestacion _ETL_API/` — Ingesta desde una API

- `dag_pokemon.py`: consulta la API pública PokeAPI, transforma algunos campos del Pokémon y muestra el resultado final. Usa la librería `requests` y archivos temporales en `/tmp`.

## Preparar el ambiente (Raspberry Pi / HomeLab)

Estos pasos describen una instalación de aprendizaje en Raspberry Pi OS o Debian de 64 bits. Airflow debe instalarse en una versión de Python compatible con la versión de Airflow elegida. Consulta los [prerrequisitos oficiales](https://airflow.apache.org/docs/apache-airflow/stable/installation/prerequisites.html) antes de instalarlo. En una Raspberry Pi, la instalación puede tardar y algunos paquetes podrían necesitar dependencias del sistema.

### 1. Instalar Python y crear un entorno virtual

En Debian o Raspberry Pi OS:

```bash
sudo apt update
sudo apt install -y python3 python3-venv python3-pip
```

Crea un entorno virtual separado para Airflow y actívalo:

```bash
python3 -m venv ~/airflow-venv
source ~/airflow-venv/bin/activate
python -m pip install --upgrade pip
```

Activa este mismo entorno cada vez que vayas a utilizar los comandos `airflow`, incluso después de reiniciar la Raspberry Pi:

```bash
source ~/airflow-venv/bin/activate
airflow standalone
```

### 2. Instalar Apache Airflow

Usa una versión explícita de Airflow y el archivo de constraints oficial correspondiente. Los constraints fijan versiones de dependencias que se han probado con esa versión de Airflow. El siguiente ejemplo usa Airflow 3.4.0; si eliges otra versión, cambia `AIRFLOW_VERSION` y usa el archivo de constraints de esa misma versión y de tu Python:

```bash
export AIRFLOW_VERSION=3.4.0
export PYTHON_VERSION="$(python -c 'import sys; print(f"{sys.version_info.major}.{sys.version_info.minor}")')"
export CONSTRAINT_URL="https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt"

pip install "apache-airflow==${AIRFLOW_VERSION}" --constraint "${CONSTRAINT_URL}"
```

Verifica que la instalación no tenga dependencias incompatibles:

```bash
pip check
```

El DAG de PokeAPI necesita `requests`. Instálalo manteniendo fijada la versión de Airflow para evitar que `pip` la cambie:

```bash
pip install "apache-airflow==${AIRFLOW_VERSION}" requests
```

Consulta la [guía oficial de instalación](https://airflow.apache.org/docs/apache-airflow/stable/installation/installing-from-pypi.html) para revisar las versiones de Python compatibles y los constraints disponibles.

### 3. Inicializar y arrancar Airflow

Con el entorno virtual todavía activo, ejecuta:

```bash
airflow standalone
```

El modo `standalone` inicia los componentes de Airflow para desarrollo y pruebas, crea la configuración, la base de datos SQLite y la carpeta de trabajo bajo `~/airflow` (salvo que configures `AIRFLOW_HOME`). En el primer inicio, la terminal muestra las credenciales de `admin`. Déjalo ejecutándose mientras uses Airflow; para detenerlo, pulsa `Ctrl+C`.

Si prefieres comprobar la carpeta de DAGs configurada, abre otra terminal, activa el mismo entorno virtual y ejecuta:

```bash
source ~/airflow-venv/bin/activate
airflow config get-value core dags_folder
```

Por defecto suele ser `~/airflow/dags`. Desde la raíz de este repositorio, copia allí **solo el DAG que quieras probar**. Por ejemplo:

```bash
cp SECCION_2/primer_dag.py ~/airflow/dags/
```

Airflow busca los DAGs en esa carpeta y los muestra en la interfaz web. No es necesario copiar el repositorio completo. Algunos ejercicios están incompletos o requieren ajustes y podrían aparecer como errores de importación si se cargan directamente.

Para comprobar qué DAGs detectó Airflow y revisar errores de importación:

```bash
airflow dags list
airflow dags list-import-errors
```

### 4. Abrir la interfaz y ejecutar un DAG

Desde el propio Raspberry Pi, abre `http://localhost:8080`. Desde otro equipo conectado a tu red local, usa `http://<IP-DE-LA-RASPBERRY>:8080`. Inicia sesión con el usuario `admin` y la contraseña que generó `airflow standalone` en el primer inicio.

En la interfaz, busca el DAG, actívalo si está pausado y ejecútalo manualmente o espera a su programación. Puedes revisar el estado y los logs de cada tarea desde el detalle de la ejecución.

No publiques el puerto 8080 directamente en Internet: `standalone` está pensado para desarrollo y aprendizaje, no para producción. Si accedes desde otro dispositivo, limita el acceso a tu red de confianza.

### Notas para estos ejercicios

- Los ejemplos pueden requerir distintas versiones de Airflow. En particular, el uso de `airflow.sdk.Asset` en el pipeline de datasets necesita una versión que incluya esa API. Comprueba las importaciones si utilizas una versión anterior.
- En `dag_ingesta.py`, `dag_transform.py` y `dag_carga.py`, revisa la ruta absoluta que se añade a `sys.path` y ajústala a la ubicación real de `datasets.py` en tu Raspberry Pi.
- El pipeline de datasets y el DAG de Pokémon leen o escriben archivos en `/tmp`. En el pipeline, las tareas deben ejecutarse en un entorno donde esos archivos estén disponibles.
- La consulta a PokeAPI necesita conexión a Internet y el paquete `requests`.
- Los ejemplos de colas y pools requieren que esos recursos estén configurados en Airflow y que el ejecutor utilizado los procese.
- El repositorio es material didáctico, no una configuración de despliegue completa. Para una instalación productiva, sigue la [documentación de despliegue de Airflow](https://airflow.apache.org/docs/apache-airflow/stable/administration-and-deployment/production-deployment.html).

## Público recomendado

El material está pensado para estudiantes, desarrolladores Python, analistas de datos e ingenieros de datos que quieran iniciarse en la orquestación de procesos y construir pipelines reproducibles.
