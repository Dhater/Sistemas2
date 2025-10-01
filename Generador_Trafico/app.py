import os
import time
import json
import pandas as pd
import numpy as np
import redis
import psycopg2
from datetime import datetime
import requests
from itertools import cycle

# 🔹 Ejecutar ingresar.py antes de nada
import ingresar
ingresar.main()  # ejecutar la función main() de ingresar.py

# 🔹 Configurar Grok
GROK_KEYS = os.getenv("OPENROUTER_API_KEY", "").split(",")  # múltiples keys separadas por coma
api_keys_cycle = cycle(GROK_KEYS)
session = requests.Session()

def call_grok(prompt, max_retries=3, base_wait=2):
    """
    Llamada no recursiva a la API; rota keys y hace backoff.
    Devuelve texto (tal cual) o lanza excepción si no hay respuesta.
    """
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
            wait = base_wait * tried
            print(f"Error con key {key}: {e}. Reintentando en {wait}s (intento {tried}/{max_retries})")
            time.sleep(wait)
    raise RuntimeError(f"Todas las reintentos fallaron: {last_exc}")

# 🔹 Intentar saludo a Grok y guardar estado en JSON
def grok_saludo():
    salida = {"status": "failed", "message": ""}
    try:
        respuesta = call_grok("Hola Grok! ¿Puedes saludarme y decir si estás online?")
        salida["status"] = "success"
        salida["message"] = respuesta
    except Exception as e:
        salida["message"] = str(e)

    # Guardar JSON
    output_path = "/data/grok_status.json"
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(salida, f, ensure_ascii=False, indent=2)
    print(f"📄 Estado de Grok guardado en {output_path}")

# 🔹 Clase de generación de tráfico
class TrafficGenerator:
    def __init__(self):
        self.df = pd.read_csv('/data/yahoo_answers.csv')
        self.redis_client = redis.Redis(
            host=os.getenv('REDIS_HOST', 'cache'), 
            port=6379, 
            decode_responses=True
        )
        self.db_conn = psycopg2.connect(
            host=os.getenv('DB_HOST', 'database'),
            database=os.getenv('DB_NAME', 'yahoo_qa'),
            user=os.getenv('DB_USER', 'postgres'),
            password=os.getenv('DB_PASSWORD', 'password123')
        )
        
    def poisson_distribution(self, lambda_param=1.0):
        return np.random.poisson(lambda_param)
    
    def uniform_distribution(self, min_time=0.1, max_time=2.0):
        return np.random.uniform(min_time, max_time)
    
    def get_random_question(self):
        return self.df.sample(n=1).iloc[0]
    
    def simulate_traffic(self):
        distribution = os.getenv('TRAFFIC_DISTRIBUTION', 'poisson')
        num_queries = int(os.getenv('NUM_QUERIES', 10000))
        
        for i in range(num_queries):
            wait_time = self.poisson_distribution() if distribution == 'poisson' else self.uniform_distribution()
            time.sleep(wait_time)
            
            question_data = self.get_random_question()
            question = question_data['Question']
            human_answer = question_data['Answer']
            
            print(f"Query {i}: {question[:100]}...")
            
        print("Simulación de tráfico completada")

if __name__ == "__main__":
    # 🔹 Saludar a Grok primero
    grok_saludo()

    # 🔹 Ejecutar tráfico
    generator = TrafficGenerator()
    generator.simulate_traffic()
