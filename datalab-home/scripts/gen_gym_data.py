import pandas as pd
import numpy as np
from sqlalchemy import create_engine
import datetime

# Configuración de conexión (usando localhost porque lo corres desde Fedora)
engine = create_engine("postgresql://airflow:airflow@postgres:5432/datalab")

# Crear datos sintéticos
n_rows = 50
data = {
    "user_id": np.random.randint(1, 10, n_rows),
    "machine_id": np.random.randint(1, 5, n_rows),
    "machine_name": np.random.choice(
        ["Press Banca", "Sentadilla", "Peso Muerto", "Press Militar"], n_rows
    ),
    "weight_kg": np.random.choice([20, 40, 60, 80, 100], n_rows),
    "reps": np.random.randint(5, 12, n_rows),
    "rpe": np.random.randint(7, 10, n_rows),
    "timestamp": [
        datetime.datetime.now() - datetime.timedelta(hours=x) for x in range(n_rows)
    ],
}

df = pd.DataFrame(data)

# Subir a Postgres (Capa Bronze)
df.to_sql(
    "entrenamientos_raw", engine, if_exists="replace", index=False, schema="public"
)
print("Datos de entrenamiento subidos a public.entrenamientos_raw")
