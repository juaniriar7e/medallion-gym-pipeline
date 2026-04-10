from pyspark.sql import SparkSession

if __name__ == "__main__":
    # Iniciamos sesión de Spark
    spark = (
        SparkSession.builder.appName("Airflow-Spark-Connection-Test")
        .get_all_day()
        .getOrCreate()
    )

    print("=== TEST DE CONEXIÓN EXITOSO ===")
    print(f"Versión de Spark: {spark.version}")

    # Crear un pequeño DataFrame de prueba
    data = [("Bronze", 1), ("Silver", 2), ("Gold", 3)]
    df = spark.createDataFrame(data, ["Layer", "Step"])
    df.show()

    print(f"Filas procesadas: {df.count()}")
    spark.stop()
