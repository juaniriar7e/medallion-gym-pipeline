import json, os, random, sys
from datetime import datetime
from uuid import uuid4

# Ruta relativa al volumen montado en Docker
OUTPUT_DIR = "./data/raw/sensors"
os.makedirs(OUTPUT_DIR, exist_ok=True)


def run_generator(count=10):
    for _ in range(count):
        # 20% de probabilidad de generar un dato "corrupto" para probar resiliencia
        is_bad = random.random() < 0.20
        data = {
            "id": str(uuid4()),
            "sensor_id": f"SENSOR_{random.randint(1, 10)}",
            "timestamp": datetime.now().isoformat(),
            "temperature": random.choice([None, 999.9, -500.0])
            if is_bad
            else random.uniform(20.0, 35.0),
            "pressure": random.uniform(14.2, 14.8),
            "status": "SENSOR_ERROR" if is_bad else "OPERATIONAL",
        }
        filename = f"log_{uuid4().hex[:6]}.json"
        with open(os.path.join(OUTPUT_DIR, filename), "w") as f:
            json.dump(data, f)
    print(f"{count} archivos JSON generados en {OUTPUT_DIR}")


if __name__ == "__main__":
    num = int(sys.argv[1]) if len(sys.argv) > 1 else 10
    run_generator(num)
