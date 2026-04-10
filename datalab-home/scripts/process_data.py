from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# Builder optimizado para Spark 4.1.1
spark = SparkSession.builder \
    .appName("Industrial-ELT-Pipeline") \
    .config("spark.jars", "/opt/bitnami/spark/extra-jars/postgresql-42.7.2.jar") \
    .getOrCreate()

try:
    # BRONZE: Lectura (Asegúrate que existan archivos en esta ruta)
    # Si la carpeta está vacía, spark.read.json lanzará error.
    df_bronze = spark.read.json("/data/raw/sensors/*.json")

    # SILVER: Limpieza de rangos industriales
    df_silver = df_bronze.filter(
        (F.col("temperature").isNotNull()) &
        (F.col("temperature").between(-50, 100))
    )

    # GOLD: Métricas Agregadas
    df_gold = df_silver.groupBy("sensor_id").agg(
        F.avg("temperature").alias("avg_temp"),
        F.max("temperature").alias("max_temp"),
        F.count("id").alias("reading_count")
    ).withColumn("processed_at", F.current_timestamp())

    # CARGA A POSTGRES 16
    print("Enviando datos a Postgres en datalab...")
    df_gold.write \
        .format("jdbc") \
        .option("url", "jdbc:postgresql://postgres:5432/datalab") \
        .option("dbtable", "gold_sensor_metrics") \
        .option("user", "airflow") \
        .option("password", "airflow") \
        .option("driver", "org.postgresql.Driver") \
        .mode("append") \
        .save()

    print("Pipeline Medallion completado.")

except Exception as e:
    print(f"Error detectado: {str(e)}")
    raise e
finally:
    spark.stop() # Siempre cierra la sesión para liberar recursos en el cluster
