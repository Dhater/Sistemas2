import os
import time
import json
import numpy as np
import requests
from datetime import datetime
from confluent_kafka import Producer, KafkaException

# --- Configuración de API y Kafka ---
API_PORT = 8000
API_URL = f"http://API_CLIENT:{API_PORT}/evaluate"

KAFKA_BROKER = os.getenv("KAFKA_BROKER", "kafka:9092")
TOPIC_PREGUNTAS = os.getenv("KAFKA_TOPIC_PREGUNTAS", "preguntas")

# --- Conexión robusta a Kafka ---
producer = None
while not producer:
    try:
        producer = Producer({'bootstrap.servers': KAFKA_BROKER})
        print("✅ Conexión con Kafka establecida")
    except KafkaException as e:
        print(f"⏳ Kafka no disponible, esperando 3s... ({e})")
        time.sleep(3)

def send_to_kafka(qid, retries=0):
    """Envía el ID (qid) al tópico de Kafka."""
    try:
        payload = json.dumps({"id": qid, "retries": retries})
        producer.produce(TOPIC_PREGUNTAS, payload.encode('utf-8'))
        producer.flush()
        print(f"📤 Enviada pregunta {qid} a Kafka topic '{TOPIC_PREGUNTAS}' (retries={retries})")
    except Exception as e:
        print(f"❌ Error enviando {qid} a Kafka: {e}")

# --- Clase principal ---
class TrafficGenerator:
    SUCCESS_THRESHOLD = 0.7
    MAX_RETRIES = 3

    def __init__(self, start_id, end_id, distribution="uniform"):
        self.start_id = start_id
        self.end_id = end_id
        self.distribution = distribution.lower()
        self.responses = []
        self.session = requests.Session()

        # Seguimiento de estados
        self.success = {}
        self.pending = set()
        self.failed = {}
        self.in_process = set()  # IDs que están siendo procesadas actualmente

    def sample_qid(self):
        """Genera un ID de pregunta según la distribución seleccionada"""
        if self.distribution == "uniform":
            return int(np.random.randint(self.start_id, self.end_id + 1))
        elif self.distribution == "normal":
            mean = (self.start_id + self.end_id) / 2
            std = (self.end_id - self.start_id) / 6
            return int(np.clip(int(np.random.normal(mean, std)), self.start_id, self.end_id))
        elif self.distribution == "poisson":
            lam = (self.start_id + self.end_id) / 2
            return int(np.clip(int(np.random.poisson(lam)), self.start_id, self.end_id))
        elif self.distribution == "random":
            return int(np.random.random() * (self.end_id - self.start_id + 1)) + self.start_id

    def get_from_api(self, qid, max_retries=3):
        """Llama a la API para obtener la evaluación de la pregunta"""
        payload = {"id": qid}
        for attempt in range(1, max_retries + 1):
            try:
                resp = self.session.post(API_URL, json=payload, timeout=60)
                resp.raise_for_status()
                data = resp.json()
                return {"request": payload, "response": data}
            except Exception as e:
                print(f"❌ Error llamando API para id={qid} (intento {attempt}): {e}")
            time.sleep(2)
        return {"request": payload, "response": None}

    def simulate_traffic(self, batch_size=5):
        """Simula el tráfico enviando IDs a Kafka y recolectando resultados"""
        # Agregar inicialmente batch de preguntas
        for _ in range(batch_size):
            qid = self.sample_qid()
            if qid not in self.pending and qid not in self.in_process:
                self.pending.add(qid)
                send_to_kafka(qid)

        # Mientras haya preguntas pendientes o en proceso, seguir procesando
        while self.pending or self.in_process:
            for qid in list(self.pending):
                self.pending.discard(qid)
                self.in_process.add(qid)

                result = self.get_from_api(qid)
                self.responses.append(result)
                overall = result.get("response", {}).get("overall_score", 0)

                if overall >= self.SUCCESS_THRESHOLD:
                    self.success[qid] = result
                    self.in_process.discard(qid)
                elif result.get("response") is None:
                    # Si no hubo respuesta, se vuelve a enviar a Kafka
                    send_to_kafka(qid, retries=0)
                else:
                    self.failed[qid] = result
                    self.in_process.discard(qid)

                print(f"ID={qid} | Overall={overall:.2f} | Pending={len(self.pending)} | In process={len(self.in_process)}")
            time.sleep(1)  # Espera para no saturar la API

        print("✅ Todas las preguntas procesadas")
        return {
            "success": list(self.success.values()),
            "pending": list(self.pending),
            "failed": list(self.failed.values())
        }

def convert_datetimes(obj):
    """Convierte todos los datetime a ISO string"""
    if isinstance(obj, dict):
        return {k: convert_datetimes(v) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [convert_datetimes(v) for v in obj]
    elif isinstance(obj, datetime):
        return obj.isoformat()
    else:
        return obj

# --- Main ---
if __name__ == "__main__":
    generator = TrafficGenerator(start_id=25000, end_id=30000, distribution="uniform")
    results = generator.simulate_traffic(batch_size=5)

    results_clean = convert_datetimes(results)
    with open("traffic_responses.json", "w", encoding="utf-8") as f:
        json.dump(results_clean, f, ensure_ascii=False, indent=2)

    print("📁 Resultados guardados en traffic_responses.json")
