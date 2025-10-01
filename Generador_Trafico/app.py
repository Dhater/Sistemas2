import os
import time
import numpy as np
import redis
import psycopg2
from itertools import cycle
import requests

# 🔹 Ejecutar ingresar.py antes de nada
import ingresar
ingresar.main()

# 🔹 Configurar Grok
GROK_KEYS = os.getenv("OPENROUTER_API_KEY", "").split(",")
api_keys_cycle = cycle(GROK_KEYS)
session = requests.Session()

def call_grok(prompt, max_retries=3, base_wait=2):
    tried = 0
    last_exc = None
    while tried < max_retries:
        key = next(api_keys_cycle)
        payload = {
            "model": "x-ai/grok-4-fast:free",
            "messages": [{"role": "user", "content": [{"type": "text", "text": prompt}]}]
        }
        try:
            resp = session.post(
                "https://openrouter.ai/api/v1/chat/completions",
                headers={"Authorization": f"Bearer {key}"},
                json=payload,
                timeout=60
            )
            resp.raise_for_status()
            data = resp.json()
            content = data["choices"][0]["message"]["content"]
            if isinstance(content, list):
                return " ".join([c.get("text", "") for c in content])
            return content
        except Exception as e:
            last_exc = e
            tried += 1
            time.sleep(base_wait * tried)
    raise RuntimeError(f"Todas las reintentos fallaron: {last_exc}")

class TrafficGeneratorDB:
    def __init__(self):
        self.redis_client = redis.Redis(
            host=os.getenv('REDIS_HOST', 'cache'),
            port=int(os.getenv('REDIS_PORT', 6379)),
            decode_responses=True
        )
        # Limpiar cache al inicio
        self.redis_client.flushdb()
        self.conn = psycopg2.connect(
            host=os.getenv('DB_HOST', 'database'),
            database=os.getenv('DB_NAME', 'yahoo_qa'),
            user=os.getenv('DB_USER', 'postgres'),
            password=os.getenv('DB_PASSWORD', 'password123')
        )
        self.conn.autocommit = True
        self.hits = 0
        self.misses = 0
        self.logs = []  # Aquí guardamos todo

    def simulate_traffic(self, num_queries=100, key_range=(1, 5000)):
        for i in range(num_queries):
            random_id = np.random.randint(key_range[0], key_range[1]+1)
            cache_key = f"question:{random_id}"

            cached = self.redis_client.get(cache_key)
            if cached:
                self.hits += 1
                question_text = cached
                hit_status = True
            else:
                self.misses += 1
                with self.conn.cursor() as cur:
                    cur.execute("SELECT question_text, llm_answer FROM questions WHERE id = %s", (random_id,))
                    row = cur.fetchone()
                    if row:
                        question_text, llm_answer = row
                        if not llm_answer:
                            llm_answer = call_grok(question_text)
                            cur.execute(
                                "UPDATE questions SET llm_answer = %s, evaluated_at = NOW() WHERE id = %s", 
                                (llm_answer, random_id)
                            )
                        self.redis_client.set(cache_key, llm_answer)
                        question_text = llm_answer
                    else:
                        question_text = None
                hit_status = False

            # Guardar en logs
            self.logs.append(f"[{i+1}] ID: {random_id} | Cache Hit: {hit_status} | Question: {question_text[:100] if question_text else 'N/A'}")

        # Resumen final
        total = self.hits + self.misses
        hit_rate = (self.hits / total) * 100 if total > 0 else 0
        miss_rate = (self.misses / total) * 100 if total > 0 else 0
        self.logs.append("\n=== Simulación completa ===")
        self.logs.append(f"Hits: {self.hits}, Misses: {self.misses}")
        self.logs.append(f"Hit Rate: {hit_rate:.2f}%, Miss Rate: {miss_rate:.2f}%")

        # Guardar en archivo
        output_file = "/data/traffic_analysis.txt"
        with open(output_file, "w", encoding="utf-8") as f:
            f.write("\n".join(self.logs))

if __name__ == "__main__":
    generator = TrafficGeneratorDB()
    generator.simulate_traffic(num_queries=1000)
