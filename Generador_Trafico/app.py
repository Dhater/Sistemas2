import os
import time
import numpy as np
import json
import requests
from datetime import datetime

API_PORT = 8000
API_URL = f"http://API_CLIENT:{API_PORT}/evaluate"

class TrafficGenerator:
    def __init__(self, start_id, end_id, distribution="uniform"):
        self.start_id = start_id
        self.end_id = end_id
        self.distribution = distribution.lower()
        self.responses = []
        self.session = requests.Session()

    def sample_qid(self):
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
        payload = {"id": qid}
        try:
            resp = self.session.post(API_URL, json=payload, timeout=60)
            resp.raise_for_status()
            return {"request": payload, "response": resp.json()}
        except Exception as e:
            print(f"❌ Error llamando API para id={qid}: {e}")
            return {"request": payload, "response": None, "error": str(e)}

    def simulate_traffic(self, num_queries=100):
        for i in range(num_queries):
            qid = self.sample_qid()
            result = self.get_from_api(qid)
            self.responses.append(result)
            print(f"[{i+1}] ID={qid} | Obtenida de API")

        print(f"✅ Simulación completa: {len(self.responses)} respuestas obtenidas")
        return self.responses


if __name__ == "__main__":
    generator = TrafficGenerator(start_id=25000, end_id=30000, distribution="uniform")
    responses = generator.simulate_traffic(num_queries=2)  # puedes cambiar el número

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

    print(f"Respuestas guardadas en {output_file}")
