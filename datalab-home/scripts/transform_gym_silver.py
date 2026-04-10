from pyspark.sql import SparkSession
from pyspark.sql.functions import col, upper, months_between, current_date, when
import sys

spark = SparkSession.builder.appName("GymSilverTransformation").getOrCreate()

jdbc_url = "jdbc:postgresql://postgres:5432/datalab"
connection_properties = {
    "user": "airflow",
    "password": "airflow",
    "driver": "org.postgresql.Driver",
}

try:
    # 1. Leer de Bronze (Postgres)
    df_bronze = spark.read.jdbc(
        url=jdbc_url, table="public.maquinas_gym", properties=connection_properties
    )

    # 2. Transformaciones Silver
    df_silver = (
        df_bronze.withColumn("estado", upper(col("estado")))
        .withColumn(
            "meses_desde_mantenimiento",
            months_between(current_date(), col("ultima_mantenimiento")).cast("int"),
        )
        .withColumn(
            "necesita_revision",
            when(col("meses_desde_mantenimiento") >= 6, True).otherwise(False),
        )
        .filter(col("resistencia_max_lbs") > 0)
    )

    # 3. Escribir en Silver (Postgres)
    df_silver.write
    # ... (resto del código igual)
    # 3. Escribir en Silver (Postgres)
    df_silver.write.format("jdbc").option("url", jdbc_url).option(
        "dbtable", "public.silver_maquinas"
    ).option("user", connection_properties["user"]).option(
        "password", connection_properties["password"]
    ).option("driver", connection_properties["driver"]).option("truncate", "true").mode(
        "overwrite"
    ).save()

    print("Capa Silver procesada y guardada en Postgres.")

except Exception as e:
    print(f"Error en Silver: {e}")
    sys.exit(1)
finally:
    spark.stop()
