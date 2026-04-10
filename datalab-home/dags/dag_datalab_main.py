from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime, timedelta
import os

default_args = {
    "owner": "juan_pablo",
    "depends_on_past": False,
    "start_date": datetime(2026, 4, 1),
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}

with DAG(
    dag_id="02_industrial_sensor_elt",
    default_args=default_args,
    description="Pipeline Medallion para datos de sensores industriales",
    schedule_interval=None,
    catchup=False,
    tags=["industrial", "pyspark"],
) as dag:

    execute_sensor_pipeline = SparkSubmitOperator(
        task_id="run_sensor_elt",
        conn_id="spark_new",
        spark_binary="/opt/spark_dist/bin/spark-submit",
        application="/opt/airflow/scripts/process_data.py",
        name="sensor_elt_job",
        # Quitamos jars de aquí si ya lo pasamos en conf para evitar duplicidad pesada
        conf={
            "spark.driver.extraClassPath": "/opt/airflow/plugins/postgresql-42.7.2.jar",
            "spark.executor.extraClassPath": "/opt/bitnami/spark/extra-jars/postgresql-42.7.2.jar",

            # CAMBIO CLAVE: Usar el nombre exacto del contenedor/servicio
            "spark.driver.host": "airflow-scheduler",
            "spark.driver.port": "7000",
            "spark.blockManager.port": "7001",
            "spark.driver.bindAddress": "0.0.0.0",

            # Recursos optimizados para evitar el bucle de EXITED
            "spark.executor.memory": "1g",
            "spark.executor.cores": "1",
            "spark.cores.max": "1", # Limita la app a 1 solo core para que no sature

            "spark.sql.shuffle.partitions": "2",
            "spark.default.parallelism": "2",
        },
        env_vars={
            "SPARK_HOME": "/opt/spark_dist",
            "JAVA_HOME": "/usr/lib/jvm/java-17-openjdk-amd64",
            "PYSPARK_PYTHON": "/usr/bin/python3"
        },
        verbose=True,
    )
