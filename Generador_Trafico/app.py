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

        # Obtener política de remoción de Redis
        self.eviction_policy = self.redis_client.config_get("maxmemory-policy").get("maxmemory-policy", "unknown")

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
        """Cantidad de keys en Redis que corresponden a preguntas"""
        return len(self.redis_client.keys("question:*"))

    def cache_memory(self):
        """Devuelve memoria usada y límite en bytes"""
        info = self.redis_client.info("memory")
        used = info.get("used_memory", 0)
        limit = info.get("maxmemory", 0)
        return used, limit

    def bytes_to_mb(self, b):
        return b / (1024*1024)

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

        total = self.hits + self.misses
        hit_rate = (self.hits / total) * 100 if total > 0 else 0
        miss_rate = (self.misses / total) * 100 if total > 0 else 0
        used_mem, max_mem = self.cache_memory()
        used_mb, max_mb = self.bytes_to_mb(used_mem), self.bytes_to_mb(max_mem)

        # Guardar logs y respuestas
        os.makedirs("/data/graficos", exist_ok=True)
        log_file = os.path.join("/data/graficos", f"traffic_logs_{self.distribution}.txt")
        resp_file = os.path.join("/data/graficos", f"traffic_responses_{self.distribution}.json")
        with open(log_file, "w", encoding="utf-8") as f:
            f.write("\n".join(self.logs))
            f.write(f"\n=== Simulación completa ({self.distribution}) ===\n"
                    f"Hits={self.hits}, Misses={self.misses}, "
                    f"HitRate={hit_rate:.2f}%, MissRate={miss_rate:.2f}%, "
                    f"Keys en cache={self.cache_size()}, "
                    f"Memoria usada={used_mb:.2f} MB / {max_mb:.2f} MB, "
                    f"Política de remoción={self.eviction_policy}\n")
        with open(resp_file, "w", encoding="utf-8") as f:
            json.dump(self.responses, f, ensure_ascii=False, indent=2, default=str)

        print(f"✅ Simulación completa para {self.distribution}. Logs guardados en {log_file}")

        # Retornar métricas para comparación
        return {
            "distribution": self.distribution,
            "queries": num_queries,
            "hits": self.hits,
            "misses": self.misses,
            "hit_rate": hit_rate,
            "miss_rate": miss_rate,
            "keys_in_cache": self.cache_size(),
            "used_memory_bytes": used_mem,
            "max_memory_bytes": max_mem,
            "eviction_policy": self.eviction_policy
        }


if __name__ == "__main__":
    import graficador  # tu script de graficar

    all_metrics = []

    for distri in ["uniform", "normal", "poisson", "random"]:
        generator = TrafficGenerator(start_id=1, end_id=10000, distribution=distri)
        metrics = generator.simulate_traffic(num_queries=10000)
        all_metrics.append(metrics)
        graficador.main(distri)

    # Limpiar la caché al final
    redis_client = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, db=0, decode_responses=True)
    redis_client.flushall()
    print("🧹 Caché limpiada después de la simulación")

    # Crear archivo comparativo entre las 4 distribuciones
    os.makedirs("/data/graficos", exist_ok=True)
    comparison_file = "/data/graficos/comparacion_distribuciones.txt"
    with open(comparison_file, "w", encoding="utf-8") as f:
        f.write("Comparación de distribuciones | Memoria constante\n\n")
        for m in all_metrics:
            used_mb = m['used_memory_bytes'] / (1024*1024)
            max_mb = m['max_memory_bytes'] / (1024*1024)
            f.write(
                f"Distribución: {m['distribution']}\n"
                f"Total queries: {m['queries']}\n"
                f"Hits: {m['hits']} | Misses: {m['misses']}\n"
                f"Hit rate: {m['hit_rate']:.2f}% | Miss rate: {m['miss_rate']:.2f}%\n"
                f"Keys en cache: {m['keys_in_cache']}\n"
                f"Memoria usada: {used_mb:.2f} MB / {max_mb:.2f} MB\n"
                f"Política de remoción: {m['eviction_policy']}\n"
                "----------------------------------------\n"
            )
    print(f"✅ Archivo comparativo guardado en {comparison_file}")
