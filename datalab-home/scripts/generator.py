import csv
import random
from datetime import datetime, timedelta


def generate_gym_data(records=100):
    marcas = ["Cybex", "Star Trac", "Nautilus", "StairMaster"]
    modelos = ["Eagle NX", "Leverage", "Nitro", "Gauntlet"]

    with open("maquinas_raw.csv", "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(
            [
                "id_maquina",
                "marca",
                "modelo",
                "resistencia_max_lbs",
                "ultima_mantenimiento",
                "estado",
            ]
        )

        for i in range(1, records + 1):
            marca = random.choice(marcas)
            # Simulamos que las Cybex suelen tener mayor resistencia en este dataset
            res_base = 300 if marca == "Cybex" else 200

            writer.writerow(
                [
                    f"M-{i:03}",
                    marca,
                    random.choice(modelos),
                    res_base + random.randint(0, 100),
                    (datetime.now() - timedelta(days=random.randint(0, 365))).strftime(
                        "%Y-%m-%d"
                    ),
                    random.choice(["Operativa", "Mantenimiento", "Fuera de Servicio"]),
                ]
            )


if __name__ == "__main__":
    generate_gym_data(150)
    print("CSV generado: maquinas_raw.csv")
