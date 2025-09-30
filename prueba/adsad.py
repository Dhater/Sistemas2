#!/usr/bin/env python3
import os
import json
import logging
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm
import psycopg2
from psycopg2.pool import SimpleConnectionPool
import redis
import pandas as pd
import requests
from dotenv import load_dotenv
import time  # Para la pausa entre lotes

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

load_dotenv()

MAX_WORKERS = int(os.getenv("MAX_WORKERS", 10))
BATCH_SIZE = int(os.getenv("BATCH_SIZE", 10))
CSV_PATH = os.getenv("CSV_PATH", "/data/yahoo_answers.csv")
LOCAL_JSON_PATH = os.getenv("LOCAL_JSON_PATH", "/data/responses.json")
LLM_JSON_PATH = os.getenv("LLM_JSON_PATH", "/data/LLM_answer.json")
API_KEYS = [k.strip() for k in os.getenv("OPENROUTER_API_KEY", "").split(",") if k.strip()]

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
        """Llama a Grok en OpenRouter"""
        key = self.api_keys[self.key_index]
        url = "https://openrouter.ai/api/v1/chat/completions"
        headers = {
            "Authorization": f"Bearer {key}",
            "Content-Type": "application/json",
            "HTTP-Referer": "http://localhost",
            "X-Title": "Yahoo_LLM_Client"
        }
        payload = {
            "model": "x-ai/grok-4-fast:free",
            "messages": [
                {
                    "role": "user",
                    "content": [
                        {"type": "text", "text": question}
                    ]
                }
            ]
        }
        try:
            resp = requests.post(url, headers=headers, data=json.dumps(payload), timeout=30)
            resp.raise_for_status()
            content = resp.json()["choices"][0]["message"]["content"][0]["text"]
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
                    cur.execute("UPDATE questions SET llm_answer=%s WHERE id=%s", (llm_answer, row[0]))
                else:
                    cur.execute(
                        "INSERT INTO questions (question_text, human_answer, llm_answer) VALUES (%s,%s,%s)",
                        (question, "", llm_answer)
                    )
            conn.commit()
            self._save_local_json(question, llm_answer, human_answer)
        except Exception as e:
            logger.exception(f"Error guardando pregunta '{question}' en DB: {e}")
            try:
                conn.rollback()
            except:
                pass
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

    def _update_missing_llm_answers(self):
        while True:
            conn = self._get_conn()
            try:
                with conn.cursor() as cur:
                    cur.execute("SELECT question_text FROM questions WHERE llm_answer='' LIMIT %s", (BATCH_SIZE,))
                    questions = [r[0] for r in cur.fetchall()]
            finally:
                self._put_conn(conn)

            if not questions:
                logger.info("✅ Todas las preguntas con llm_answer vacío procesadas")
                break

            with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
                futures = [executor.submit(self._process_question, q) for q in questions]
                for future in as_completed(futures):
                    question, answer = future.result()
                    logger.info(f"Procesada pregunta: {question} -> {answer[:60]}...")

            logger.info("💤 Esperando 60 segundos antes del siguiente lote...")
            time.sleep(60)

    def _fill_llm_json(self):
        if os.path.exists(LLM_JSON_PATH):
            with open(LLM_JSON_PATH, "r", encoding="utf-8") as f:
                data = json.load(f)
        else:
            data = {}

        conn = self._get_conn()
        try:
            with conn.cursor() as cur:
                cur.execute("SELECT id, question_text FROM questions")
                rows = cur.fetchall()
                for r in rows:
                    qid, qtext = r
                    if str(qid) not in data:
                        data[str(qid)] = {
                            "question_text": qtext,
                            "human_answer": "",
                            "llm_answer": None,
                            "similarity_score": None,
                            "quality_score": None,
                            "completeness_score": None,
                            "overall_score": None,
                            "created_at": datetime.now().isoformat(),
                            "evaluated_at": None
                        }
        finally:
            self._put_conn(conn)

        keys = [k for k in data if not data[k]["llm_answer"]]
        batch = []
        for k in keys:
            batch.append(k)
            if len(batch) >= BATCH_SIZE or k == keys[-1]:
                with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
                    futures = {executor.submit(self._call_llm, data[i]["question_text"]): i for i in batch}
                    for future in as_completed(futures):
                        i = futures[future]
                        try:
                            ans = future.result()
                            data[i]["llm_answer"] = ans
                            data[i]["evaluated_at"] = datetime.now().isoformat()
                        except Exception as e:
                            logger.warning(f"Error procesando pregunta {i}: {e}")
                logger.info("💤 Esperando 60 segundos antes del siguiente batch de LLM...")
                time.sleep(60)
                batch = []

        with open(LLM_JSON_PATH, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
        logger.info(f"✅ Todas las respuestas LLM actualizadas en {LLM_JSON_PATH}")

    def _ensure_all_questions_exist(self):
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

    def _backup_to_json(self):
        conn = self._get_conn()
        try:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT id, question_text, human_answer, llm_answer, similarity_score,
                           quality_score, completeness_score, overall_score,
                           created_at, evaluated_at
                    FROM questions
                """)
                rows = cur.fetchall()
                backup = {}
                for r in rows:
                    backup[r[0]] = {
                        "question_text": r[1],
                        "human_answer": r[2],
                        "llm_answer": r[3],
                        "similarity_score": r[4],
                        "quality_score": r[5],
                        "completeness_score": r[6],
                        "overall_score": r[7],
                        "created_at": r[8].isoformat() if r[8] else None,
                        "evaluated_at": r[9].isoformat() if r[9] else None
                    }
                with open(LLM_JSON_PATH, "w", encoding="utf-8") as f:
                    json.dump(backup, f, ensure_ascii=False, indent=2)
                logger.info(f"✅ Backup completo LLM guardado: {LLM_JSON_PATH}")
        finally:
            self._put_conn(conn)

    def run(self):
        self._ensure_all_questions_exist()
        self._update_missing_llm_answers()
        self._backup_to_json()
        self._fill_llm_json()


if __name__ == "__main__":
    client = LLMClient()
    client.run()
