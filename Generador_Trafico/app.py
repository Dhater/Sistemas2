import os
import time
import numpy as np
import json
import requests
from datetime import datetime
from confluent_kafka import Producer

# --- Configuración de API ---
API_PORT = 8000
API_URL = f"http://API_CLIENT:{API_PORT}/evaluate"

# --- Configuración de Kafka ---
KAFKA_BROKER = os.getenv("KAFKA_BROKER", "kafka:9092")
TOPIC_PREGUNTAS = os.getenv("KAFKA_TOPIC_PREGUNTAS", "preguntas")

producer = Producer({'bootstrap.servers': KAFKA_BROKER})

def send_to_kafka(qid):
    """Envía el ID (qid) al tópico de Kafka."""
    try:
        payload = json.dumps({"id": qid})
        producer.produce(TOPIC_PREGUNTAS, payload.encode('utf-8'))
        producer.flush()
        print(f"📤 Enviada pregunta {qid} a Kafka topic '{TOPIC_PREGUNTAS}'")
    except Exception as e:
        print(f"❌ Error enviando {qid} a Kafka: {e}")


# --- Clase principal de generador de tráfico ---
class TrafficGenerator:
    def __init__(self, start_id, end_id, distribution="uniform"):
        self.start_id = start_id
        self.end_id = end_id
        self.distribution = distribution.lower()
        self.responses = []
        self.session = requests.Session()

    def sample_qid(self):
        """Genera un ID de pregunta según la distribución elegida."""
        if self.distribution == "uniform":
            return int(np.random.randint(self.start_id, self.end_id + 1))
        elif self.distribution == "normal":
            mean = (self.start_id + self.end_id) / 2
            std = (self.end_id - self.start_id) / 6
            qid = int(np.random.normal(mean, std))
            return int(np.clip(qid, self.start_id, self.end_id))
        elif self.distribution == "poisson":
            lam = (self.start_id + self.end_id) / 2
            qid = int(np.random.poisson(lam))
            return int(np.clip(qid, self.start_id, self.end_id))
        elif self.distribution == "random":
            return int(np.random.random() * (self.end_id - self.start_id + 1)) + self.start_id

    def get_from_api(self, qid):
        """Hace una llamada HTTP al API con el ID generado."""
        payload = {"id": qid}
        try:
            resp = self.session.post(API_URL, json=payload, timeout=60)
            resp.raise_for_status()
            return {"request": payload, "response": resp.json()}
        except Exception as e:
            print(f"❌ Error llamando API para id={qid}: {e}")
            return {"request": payload, "response": None, "error": str(e)}

    def simulate_traffic(self, num_queries=100):
        """Simula tráfico: envía IDs a Kafka y hace llamadas al API."""
        for i in range(num_queries):
            qid = self.sample_qid()

            # 🔹 Enviar a Kafka
            send_to_kafka(qid)

            # 🔹 Llamar al API
            result = self.get_from_api(qid)
            self.responses.append(result)

            print(f"[{i+1}] ID={qid} | Enviada a Kafka y obtenida de API")

        print(f"✅ Simulación completa: {len(self.responses)} respuestas obtenidas")
        return self.responses


# --- Ejecución principal ---
if __name__ == "__main__":
    generator = TrafficGenerator(start_id=25000, end_id=30000, distribution="uniform")
    responses = generator.simulate_traffic(num_queries=5)  # puedes cambiar el número

    # --- Función para convertir datetimes a string ---
    def convert_datetimes(obj):
        if isinstance(obj, dict):
            return {k: convert_datetimes(v) for k, v in obj.items()}
        elif isinstance(obj, list):
            return [convert_datetimes(v) for v in obj]
        elif isinstance(obj, datetime):
            return obj.isoformat()
        else:
            return obj

    # Convertir datetimes en las respuestas
    responses_clean = convert_datetimes(responses)

    # Guardar respuestas en JSON local
    output_file = "traffic_responses.json"
    with open(output_file, "w", encoding="utf-8") as f:
        json.dump(responses_clean, f, ensure_ascii=False, indent=2)

    print(f"📁 Respuestas guardadas en {output_file}")
