from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime
from airflow.operators.bash import BashOperator

with DAG(
    dag_id="gym_data_ingestion",
    start_date=datetime(2026, 4, 5),
    schedule_interval="@daily",
    catchup=False,
) as dag:
    ingest_task = SparkSubmitOperator(
        task_id="spark_ingest_maquinas",
        application="/opt/airflow/scripts/ingest_gym.py",  # <--- ESTA ES LA RUTA QUE DIÓ EL FIND
        conn_id="spark_default",
        jars="/opt/airflow/plugins/postgresql-42.7.2.jar",
        driver_class_path="/opt/airflow/plugins/postgresql-42.7.2.jar",
        name="arrow-spark",
        conf={
            "spark.driver.extraClassPath": "/opt/airflow/plugins/postgresql-42.7.2.jar"
        },
    )
    transform_silver = SparkSubmitOperator(
        task_id="spark_transform_silver",
        application="/opt/airflow/scripts/transform_gym_silver.py",
        conn_id="spark_default",
        jars="/opt/airflow/plugins/postgresql-42.7.2.jar",
        driver_class_path="/opt/airflow/plugins/postgresql-42.7.2.jar",
    )
    # Tarea de dbt para la capa de Transformación (Silver)
    dbt_transform = BashOperator(
        task_id="dbt_transform_gym",
        # Usamos la ruta absoluta al binario de dbt dentro de tu venv
        bash_command="cd /opt/airflow/dbt/gym_project && /opt/airflow/dbt/venv/bin/dbt run",
        env={"DBT_PROFILES_DIR": "/opt/airflow/dbt/gym_project"},
    )

# Definir la secuencia: Primero Ingesta (Bronze), luego Transformación (Silver)
ingest_task >> transform_silver >> dbt_transform
