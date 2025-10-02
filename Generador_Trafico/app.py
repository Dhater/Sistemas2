import os
import time
import numpy as np
import redis
import json
import requests
import ingresar
import psycopg2
from psycopg2.extras import RealDictCursor

ingresar.main()
API_PORT = 8000
API_URL = f"http://llm_client_pruebas:{API_PORT}/evaluate"
REDIS_HOST = os.getenv('REDIS_HOST', 'cache')
REDIS_PORT = int(os.getenv('REDIS_PORT', 6379))

class TrafficGenerator:
    def __init__(self, start_id, end_id, distribution="uniform"):
        self.redis_client = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)
        self.redis_client.flushdb()
        self.start_id = start_id
        self.end_id = end_id
        self.distribution = distribution.lower()
        self.hits = 0
        self.misses = 0
        self.logs = []
        self.responses = []

        self.session = requests.Session()
        self.conn = psycopg2.connect(
            host=os.getenv("DB_HOST", "database"),
            dbname=os.getenv("DB_NAME"),
            user=os.getenv("DB_USER"),
            password=os.getenv("DB_PASSWORD"),
            cursor_factory=RealDictCursor
        )

    def get_from_db(self, qid):
        with self.conn.cursor() as cur:
            cur.execute("SELECT * FROM questions WHERE id = %s", (qid,))
            row = cur.fetchone()
            if row:
                return dict(row)
        return None

    def get_from_api(self, qid):
        try:
            resp = self.session.post(API_URL, json={"id": qid}, timeout=60)
            resp.raise_for_status()
            return resp.json()
        except Exception as e:
            print(f"❌ Error llamando API para id={qid}: {e}")
            return None

    def cache_size(self):
        """Devuelve la cantidad de keys en Redis que corresponden a preguntas"""
        return len(self.redis_client.keys("question:*"))

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

    def simulate_traffic(self, num_queries=1000):
        for i in range(num_queries):
            qid = self.sample_qid()
            cache_key = f"question:{qid}"
            cached = self.redis_client.get(cache_key)

            if cached:
                self.hits += 1
                data = json.loads(cached)
                hit_status = True
            else:
                self.misses += 1
                data = self.get_from_db(qid)
                if data:
                    self.redis_client.set(cache_key, json.dumps(data, default=str))
                else:
                    data = self.get_from_api(qid)
                    if data:
                        self.redis_client.set(cache_key, json.dumps(data))
                hit_status = False

            self.logs.append(f"[{i+1}] ID={qid} | Cache Hit={hit_status}")
            if data:
                self.responses.append(data)

            if (i+1) % 50 == 0:
                print(f"⏱ {i+1}/{num_queries} queries completadas | Hits={self.hits}, Misses={self.misses}")

        total = self.hits + self.misses
        hit_rate = (self.hits / total) * 100 if total > 0 else 0
        miss_rate = (self.misses / total) * 100 if total > 0 else 0
        self.logs.append(
            f"\n=== Simulación completa ({self.distribution}) ===\nHits={self.hits}, Misses={self.misses}, "
            f"HitRate={hit_rate:.2f}%, MissRate={miss_rate:.2f}%, Total IDs en cache={self.cache_size()}"
        )

        # Guardar logs y respuestas
        os.makedirs("/data/graficos", exist_ok=True)
        log_file = os.path.join("/data/graficos", f"traffic_logs_{self.distribution}.txt")
        resp_file = os.path.join("/data/graficos", f"traffic_responses_{self.distribution}.json")
        with open(log_file, "w", encoding="utf-8") as f:
            f.write("\n".join(self.logs))
        with open(resp_file, "w", encoding="utf-8") as f:
            json.dump(self.responses, f, ensure_ascii=False, indent=2, default=str)

        print(f"✅ Simulación completa para {self.distribution}. Logs guardados en {log_file}")


if __name__ == "__main__":
    import graficador  # tu script de graficar

    for distri in ["uniform", "normal", "poisson", "random"]:
        generator = TrafficGenerator(start_id=1, end_id=10000, distribution=distri)
        generator.simulate_traffic(num_queries=10000)
        graficador.main(distri)
