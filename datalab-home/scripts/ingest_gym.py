from pyspark.sql import SparkSession
import sys

# Inicialización de la sesión
spark = SparkSession.builder.appName("GymDataIngestion").getOrCreate()

# RUTA CRÍTICA: El Worker de Bitnami busca en /opt/bitnami/data/
# Asegúrate de que el CSV esté en ./datalab-home/data/maquinas_raw.csv en tu host
csv_path = "/opt/bitnami/data/maquinas_raw.csv"

try:
    print(f"Intentando leer: {csv_path}")
    df = spark.read.csv(csv_path, header=True, inferSchema=True)

    # 2. Escribir en Postgres
    # Usamos el nombre del servicio 'postgres' del compose
    df.write.format("jdbc").option(
        "url", "jdbc:postgresql://postgres:5432/datalab"
    ).option("dbtable", "public.maquinas_gym").option("user", "airflow").option(
        "password", "airflow"
    ).option("driver", "org.postgresql.Driver").option("truncate", "true").mode(
        "overwrite"
    ).save()
    print("¡Datos cargados en Postgres exitosamente!")

except Exception as e:
    print(f"Error durante la ejecución: {e}")
    sys.exit(1)

finally:
    spark.stop()
