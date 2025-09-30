#!/usr/bin/env python3
import os
import json
import time
import logging
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed

import psycopg2
from psycopg2.pool import SimpleConnectionPool
import redis
import pandas as pd
import requests
from dotenv import load_dotenv

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

load_dotenv()

# Configuración
MAX_WORKERS = int(os.getenv("MAX_WORKERS", 10))
BATCH_SIZE = int(os.getenv("BATCH_SIZE", 10))
CSV_PATH = os.getenv("CSV_PATH", "/data/yahoo_answers.csv")
LOCAL_JSON_PATH = os.getenv("LOCAL_JSON_PATH", "/data/responses.json")
API_KEYS = [k.strip() for k in os.getenv("OPENROUTER_API_KEY", "").split(",") if k.strip()]
MODEL = os.getenv("MODEL", "nvidia/nemotron-nano-9b-v2:free")

DB_CONFIG = {
    "host": os.getenv("DB_HOST", "database"),
    "database": os.getenv("DB_NAME", "yahoo_qa"),
    "user": os.getenv("DB_USER", "postgres"),
    "password": os.getenv("DB_PASSWORD", "password123"),
    "port": int(os.getenv("DB_PORT", 5432))
}

REDIS_CONFIG = {
    "host": os.getenv("REDIS_HOST", "cache"),
    "port": int(os.getenv("REDIS_PORT", 6379)),
    "decode_responses": True
}

class LLMClient:
    def __init__(self):
        if not API_KEYS:
            raise ValueError("No se encontraron API keys en OPENROUTER_API_KEY")
        self.api_keys = API_KEYS
        self.key_index = 0
        self.redis_client = redis.Redis(**REDIS_CONFIG)
        self.pool = SimpleConnectionPool(1, 10, **DB_CONFIG)
        self.local_data = {}
        if os.path.exists(LOCAL_JSON_PATH):
            try:
                with open(LOCAL_JSON_PATH, "r", encoding="utf-8") as f:
                    self.local_data = json.load(f)
            except Exception as e:
                logger.warning(f"No se pudo cargar JSON: {e}")

    def _rotate_key(self):
        self.key_index = (self.key_index + 1) % len(self.api_keys)
        return self.api_keys[self.key_index]

    def _call_llm(self, question: str) -> str:
        key = self.api_keys[self.key_index]
        url = "https://openrouter.ai/api/v1/chat/completions"
        headers = {"Authorization": f"Bearer {key}", "Content-Type": "application/json"}
        payload = {"model": MODEL, "messages": [{"role": "user", "content": question}]}

        try:
            resp = requests.post(url, headers=headers, json=payload, timeout=30)
            resp.raise_for_status()
            data = resp.json()
            content = data["choices"][0]["message"]["content"]
            return content.strip() if content else ""
        except Exception as e:
            logger.warning(f"Error LLM con key {key}: {e}. Rotando key...")
            self._rotate_key()
            return self._call_llm(question)

    def _get_conn(self):
        return self.pool.getconn()

    def _put_conn(self, conn):
        self.pool.putconn(conn)

    def _save_local_json(self, question, llm_answer, human_answer):
        self.local_data[question] = {
            "llm_answer": llm_answer,
            "human_answer": human_answer or "",
            "timestamp": datetime.now().isoformat()
        }
        try:
            with open(LOCAL_JSON_PATH, "w", encoding="utf-8") as f:
                json.dump(self.local_data, f, ensure_ascii=False, indent=2)
        except Exception as e:
            logger.warning(f"No se pudo guardar JSON local: {e}")

    def _save_to_db(self, question, llm_answer):
        conn = self._get_conn()
        try:
            with conn.cursor() as cur:
                cur.execute("SELECT id, human_answer FROM questions WHERE question_text=%s LIMIT 1", (question,))
                row = cur.fetchone()
                human_answer = row[1] if row else ""
                if row:
                    cur.execute(
                        "UPDATE questions SET llm_answer=%s WHERE id=%s",
                        (llm_answer, row[0])
                    )
                else:
                    cur.execute(
                        "INSERT INTO questions (question_text, human_answer, llm_answer) VALUES (%s,%s,%s)",
                        (question, "", llm_answer)
                    )
            conn.commit()
            self._save_local_json(question, llm_answer, human_answer)
        except Exception as e:
            logger.exception(f"Error guardando pregunta '{question}' en DB: {e}")
            try: conn.rollback()
            except: pass
        finally:
            self._put_conn(conn)

    def _process_question(self, question):
        cached = self.redis_client.get(question)
        if cached:
            return question, cached
        llm_answer = self._call_llm(question)
        self.redis_client.set(question, llm_answer)
        self._save_to_db(question, llm_answer)
        return question, llm_answer

    def run(self):
        # Leer CSV y actualizar DB con preguntas faltantes
        df = pd.read_csv(CSV_PATH)
        conn = self._get_conn()
        try:
            with conn.cursor() as cur:
                for q in df['question_text']:
                    cur.execute("SELECT 1 FROM questions WHERE question_text=%s", (q,))
                    if not cur.fetchone():
                        cur.execute(
                            "INSERT INTO questions (question_text, human_answer, llm_answer) VALUES (%s,%s,%s)",
                            (q, "", "")
                        )
            conn.commit()
        finally:
            self._put_conn(conn)

        # Iterar preguntas sin respuesta
        while True:
            conn = self._get_conn()
            try:
                with conn.cursor() as cur:
                    cur.execute("SELECT question_text FROM questions WHERE llm_answer='' LIMIT %s", (BATCH_SIZE,))
                    questions = [r[0] for r in cur.fetchall()]
            finally:
                self._put_conn(conn)

            if not questions:
                logger.info("✅ Todas las preguntas procesadas")
                break

            with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
                futures = [executor.submit(self._process_question, q) for q in questions]
                for future in as_completed(futures):
                    question, answer = future.result()
                    logger.info(f"Procesada pregunta: {question} -> {answer[:60]}...")

if __name__ == "__main__":
    client = LLMClient()
    client.run()
