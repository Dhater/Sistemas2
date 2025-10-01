import os
import time
import numpy as np
import redis
import json
import requests
from itertools import cycle

API_PORT = 8000
API_URL = f"http://localhost:{API_PORT}/evaluate"
REDIS_HOST = os.getenv('REDIS_HOST', 'cache')
REDIS_PORT = int(os.getenv('REDIS_PORT', 6379))

class TrafficGenerator:
    def __init__(self, start_id=20000, end_id=23000):
        self.redis_client = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)
        self.redis_client.flushdb()  # limpiar cache
        self.start_id = start_id
        self.end_id = end_id
        self.hits = 0
        self.misses = 0
        self.logs = []
        self.responses = []

        self.session = requests.Session()

    def get_from_api(self, qid):
        try:
            resp = self.session.post(API_URL, json={"id": qid}, timeout=60)
            resp.raise_for_status()
            data = resp.json()
            return data
        except Exception as e:
            print(f"❌ Error llamando API para id={qid}: {e}")
            return None

    def simulate_traffic(self, num_queries=1000):
        for i in range(num_queries):
            qid = np.random.randint(self.start_id, self.end_id + 1)
            cache_key = f"question:{qid}"
            cached = self.redis_client.get(cache_key)

            if cached:
                self.hits += 1
                data = json.loads(cached)
                hit_status = True
            else:
                self.misses += 1
                # Llamada a API FastAPI
                data = self.get_from_api(qid)
                if data:
                    self.redis_client.set(cache_key, json.dumps(data))
                hit_status = False

            self.logs.append(f"[{i+1}] ID={qid} | Cache Hit={hit_status}")
            if data:
                self.responses.append(data)

            if (i+1) % 50 == 0:
                print(f"⏱ {i+1}/{num_queries} queries completadas | Hits={self.hits}, Misses={self.misses}")

        # Resumen final
        total = self.hits + self.misses
        hit_rate = (self.hits / total) * 100 if total > 0 else 0
        miss_rate = (self.misses / total) * 100 if total > 0 else 0
        self.logs.append(f"\n=== Simulación completa ===\nHits={self.hits}, Misses={self.misses}, HitRate={hit_rate:.2f}%, MissRate={miss_rate:.2f}%")

        # Guardar logs
        os.makedirs("/data", exist_ok=True)
        with open("/data/traffic_logs.txt", "w", encoding="utf-8") as f:
            f.write("\n".join(self.logs))

        # Guardar respuestas
        with open("/data/traffic_responses.json", "w", encoding="utf-8") as f:
            json.dump(self.responses, f, ensure_ascii=False, indent=2)

        print(f"✅ Simulación completada. Logs y respuestas guardadas en /data")

if __name__ == "__main__":
    generator = TrafficGenerator(start_id=20000, end_id=23000)
    generator.simulate_traffic(num_queries=500)  # ajusta la cantidad
