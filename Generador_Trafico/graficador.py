import os
from collections import Counter
import matplotlib.pyplot as plt

BASE_DIR = os.path.dirname(__file__)
TRAFFIC_LOGS = os.path.join(BASE_DIR, "../data/traffic_logs.txt")
GRAFICOS_DIR = os.path.join(BASE_DIR, "../graficos")

os.makedirs(GRAFICOS_DIR, exist_ok=True)

def main(distribution_name="uniform"):
    # Leer archivo
    with open(TRAFFIC_LOGS, "r", encoding="utf-8") as f:
        lines = f.readlines()

    # Extraer IDs de cada línea
    ids = []
    for line in lines:
        if line.startswith("["):
            try:
                parts = line.split("ID=")
                id_part = parts[1].split(" ")[0]
                ids.append(int(id_part))
            except Exception:
                continue

    # Contar ocurrencias de cada ID
    counter = Counter(ids)
    id_list = list(counter.keys())
    count_list = list(counter.values())

    # Graficar
    plt.figure(figsize=(12, 6))
    plt.bar(id_list, count_list, color="skyblue")
    plt.xlabel("ID")
    plt.ylabel("Cantidad de Hits/Misses")
    plt.title(f"Distribución de IDs - {distribution_name}")
    plt.tight_layout()

    # Guardar gráfico
    output_path = os.path.join(GRAFICOS_DIR, f"distribucion_{distribution_name}.png")
    plt.savefig(output_path)
    plt.close()
    print(f"✅ Gráfico guardado en: {output_path}")
