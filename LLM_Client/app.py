#!/usr/bin/env python3
import os
import time
import json
import signal
import sys
import hashlib
import logging
from datetime import datetime
from typing import Optional, List
from concurrent.futures import ThreadPoolExecutor, as_completed

import redis
import psycopg2
from psycopg2.pool import SimpleConnectionPool
from dotenv import load_dotenv
from openai import OpenAI

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

load_dotenv()
TARGET_TOTAL = int(os.getenv("TARGET_TOTAL", "20000"))
EXPORT_CSV_PATH = os.getenv("EXPORT_CSV_PATH", "/data/questions_backup.csv")
LOCAL_JSON_PATH = "./local_data/responses.json"
MAX_WORKERS = int(os.getenv("MAX_WORKERS", "5"))  # Número de threads simultáneos


class LLMClient:
    def __init__(self):
        # ---------------------------
        # Manejo de múltiples API keys
        # ---------------------------
        raw_keys = os.getenv("OPENROUTER_API_KEY", "")
        self.api_keys = [k.strip() for k in raw_keys.split(",") if k.strip()]
        if not self.api_keys:
            raise ValueError("No se encontró OPENROUTER_API_KEY en el entorno")
        self.api_key_index = 0
        self._init_client()
        logger.info(f"✅ Cliente Grok/OpenRouter inicializado con {len(self.api_keys)} API keys")

        # Redis
        self.redis_client = redis.Redis(
            host=os.getenv('REDIS_HOST', 'cache'),
            port=int(os.getenv('REDIS_PORT', '6379')),
            decode_responses=True
        )

        # PostgreSQL pool
        self.db_config = {
            'host': os.getenv('DB_HOST', 'database'),
            'database': os.getenv('DB_NAME', 'yahoo_qa'),
            'user': os.getenv('DB_USER', 'postgres'),
            'password': os.getenv('DB_PASSWORD', 'password123'),
            'port': int(os.getenv('DB_PORT', '5432'))
        }

        try:
            self.pool = SimpleConnectionPool(
                minconn=int(os.getenv("DB_POOL_MIN", "1")),
                maxconn=int(os.getenv("DB_POOL_MAX", "10")),
                **self.db_config
            )
            logger.info("✅ Connection pool creado")
        except Exception as e:
            logger.warning("No se pudo crear pool, intentando conexión directa: %s", e)
            self.pool = None
            self.db_connection = self._connect_to_db()

        # Manejo señales
        signal.signal(signal.SIGINT, self._graceful_shutdown)
        signal.signal(signal.SIGTERM, self._graceful_shutdown)

        # JSON local
        if os.path.exists(LOCAL_JSON_PATH):
            try:
                with open(LOCAL_JSON_PATH, "r", encoding="utf-8") as f:
                    self.local_data = json.load(f)
            except Exception as e:
                logger.warning(f"No se pudo cargar JSON existente, se creará uno nuevo: {e}")
                self.local_data = {}
        else:
            self.local_data = {}

    # ---------------------------
    # Manejo señales y DB
    # ---------------------------
    def _graceful_shutdown(self, signum, frame):
        logger.info("Recibido señal de terminación, cerrando...")
        try:
            if hasattr(self, "pool") and self.pool:
                self.pool.closeall()
        except Exception:
            pass
        try:
            if hasattr(self, "db_connection") and self.db_connection:
                self.db_connection.close()
        except Exception:
            pass
        sys.exit(0)

    def _connect_to_db(self):
        for attempt in range(5):
            try:
                conn = psycopg2.connect(**self.db_config)
                logger.info("✅ Conectado a la base de datos (directo)")
                return conn
            except Exception as e:
                logger.warning(f"Intento {attempt+1}/5 fallo al conectar BD: {e}")
                time.sleep(5)
        raise Exception("No se pudo conectar a la base de datos")

    def _get_conn(self):
        if hasattr(self, "pool") and self.pool:
            return self.pool.getconn()
        else:
            return self.db_connection or self._connect_to_db()

    def _put_conn(self, conn):
        if hasattr(self, "pool") and self.pool:
            self.pool.putconn(conn)

    # ---------------------------
    # Cache helpers
    # ---------------------------
    def _generate_cache_key(self, question: str) -> str:
        h = hashlib.sha256(question.strip().lower().encode("utf-8")).hexdigest()
        return f"llm_answer:{h}"

    def get_cached_answer(self, question: str) -> Optional[str]:
        try:
            return self.redis_client.get(self._generate_cache_key(question))
        except Exception as e:
            logger.warning(f"⚠ Error leyendo cache: {e}")
            return None

    def cache_answer(self, question: str, answer: str, ttl: int = 3600):
        try:
            self.redis_client.setex(self._generate_cache_key(question), ttl, answer)
        except Exception as e:
            logger.warning(f"⚠ No se pudo escribir en Redis: {e}")

    # ---------------------------
    # DB helpers
    # ---------------------------
    def save_question_answer_to_db(self, question: str, llm_answer: str, human_answer: str = ""):
        conn = self._get_conn()
        try:
            with conn.cursor() as cur:
                cur.execute("SELECT id FROM questions WHERE question_text=%s LIMIT 1", (question,))
                existing = cur.fetchone()
                if existing:
                    cur.execute(
                        "UPDATE questions SET llm_answer=%s, human_answer=COALESCE(%s, human_answer) WHERE id=%s",
                        (llm_answer, human_answer or "", existing[0])
                    )
                else:
                    cur.execute(
                        "INSERT INTO questions (question_text, human_answer, llm_answer) VALUES (%s,%s,%s)",
                        (question, human_answer or "", llm_answer)
                    )
            conn.commit()
        except Exception as e:
            logger.exception("Error guardando QA en DB: %s", e)
            try:
                conn.rollback()
            except Exception:
                pass
        finally:
            self._put_conn(conn)

    def get_next_questions(self, limit: int = 20) -> List[str]:
        conn = self._get_conn()
        try:
            with conn.cursor() as cur:
                cur.execute("SELECT question_text FROM questions WHERE llm_answer IS NULL LIMIT %s", (limit,))
                return [r[0] for r in cur.fetchall()]
        finally:
            self._put_conn(conn)

    def get_human_answer_from_db(self, question: str) -> Optional[str]:
        conn = self._get_conn()
        try:
            with conn.cursor() as cur:
                cur.execute("SELECT human_answer FROM questions WHERE question_text=%s LIMIT 1", (question,))
                row = cur.fetchone()
                return row[0] if row else ""
        finally:
            self._put_conn(conn)

    # ---------------------------
    # JSON local helpers
    # ---------------------------
    def _save_to_local_json(self, question: str, llm_answer: str):
        human_answer = self.get_human_answer_from_db(question) or ""
        timestamp = datetime.now().isoformat()
        self.local_data[question] = {"llm_answer": llm_answer, "human_answer": human_answer, "timestamp": timestamp}
        try:
            os.makedirs(os.path.dirname(LOCAL_JSON_PATH), exist_ok=True)
            with open(LOCAL_JSON_PATH, "w", encoding="utf-8") as f:
                json.dump(self.local_data, f, ensure_ascii=False, indent=2)
        except Exception as e:
            logger.warning(f"⚠ No se pudo escribir JSON local: {e}")

    # ---------------------------
    # Cliente OpenRouter con rotación de keys
    # ---------------------------
    def _init_client(self):
        key = self.api_keys[self.api_key_index]
        self.client = OpenAI(base_url="https://openrouter.ai/api/v1", api_key=key)

    def _rotate_key(self):
        self.api_key_index = (self.api_key_index + 1) % len(self.api_keys)
        self._init_client()

    def _query_grok(self, question: str) -> tuple:
        try:
            completion = self.client.chat.completions.create(
                model="x-ai/grok-4-fast:free",
                messages=[{"role": "user", "content":[{"type":"text","text":f"Eres un experto en finanzas. Responde concisamente:\nPregunta: {question}\nRespuesta:"}]}]
            )
            choice = completion.choices[0]
            msg = getattr(choice, "message", None) or choice.get("message") if isinstance(choice, dict) else None
            content = getattr(msg, "content", None) or msg.get("content") if msg else getattr(choice, "text", None) or choice.get("text") if isinstance(choice, dict) else None
            ans = json.dumps(content, ensure_ascii=False) if isinstance(content, (list, dict)) else str(content or "")
        except Exception as e:
            if "401" in str(e):
                logger.warning(f"⚠ API key inválida, rotando key y reintentando: {self.api_keys[self.api_key_index]}")
                self._rotate_key()
                return self._query_grok(question)  # reintento con la siguiente key
            ans = f"Error: {e}"
        return question, ans.strip()

    def get_answers_from_grok_batch(self, questions: List[str]) -> List[tuple]:
        responses = []
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            futures = [executor.submit(self._query_grok, q) for q in questions]
            for future in as_completed(futures):
                responses.append(future.result())
        return responses

    # ---------------------------
    # Run batches con threads
    # ---------------------------
    def run_batches(self, batch_size: int = 20, max_questions: int = 150000):
        processed_count = 0
        while processed_count < max_questions:
            questions = self.get_next_questions(limit=batch_size)
            if not questions:
                logger.info("✅ No quedan preguntas sin respuesta en la BD")
                break

            responses = self.get_answers_from_grok_batch(questions)
            for q, ans in responses:
                self.save_question_answer_to_db(q, ans)
                self.cache_answer(q, ans)
                self._save_to_local_json(q, ans)
                processed_count += 1
                logger.info(f"📝 Procesada pregunta {processed_count}/{max_questions}")

        logger.info("🏁 Run_batches finalizado")

    # ---------------------------
    # Export CSV
    # ---------------------------
    def export_db_to_csv(self, file_path: str = EXPORT_CSV_PATH):
        conn = self._get_conn()
        try:
            with open(file_path, "w", encoding="utf-8") as f:
                with conn.cursor() as cur:
                    cur.copy_expert("""
                        COPY (
                            SELECT id, question_text, human_answer, llm_answer, created_at, evaluated_at
                            FROM questions
                        ) TO STDOUT WITH CSV HEADER
                    """, f)
            logger.info("✅ Exportado CSV exitosamente: %s", file_path)
        finally:
            self._put_conn(conn)


def main():
    client = LLMClient()
    client.run_batches(batch_size=20, max_questions=30000)
    client.export_db_to_csv()


if __name__ == "__main__":
    main()
