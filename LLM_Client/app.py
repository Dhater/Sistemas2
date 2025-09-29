#!/usr/bin/env python3
import os
import time
import requests
import psycopg2
import redis
import json
import logging
from datetime import datetime
from dotenv import load_dotenv
from typing import List, Optional

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

load_dotenv()

# Endpoint para Gemini Flash 2.0 (Generative Language API v1beta)
GEMINI_URL = "https://generativelanguage.googleapis.com/v1beta/models/gemini-2.0-flash:generateContent"

# Objetivo visible cuando pides "decir en qué número va de 30000"
TARGET_TOTAL = 30000


class LLMClient:
    def __init__(self):
        # Cargar hasta 5 API keys desde .env: GEMINI_API_KEY1 ... GEMINI_API_KEY5
        raw_keys = [
            os.getenv("GEMINI_API_KEY1"),
            os.getenv("GEMINI_API_KEY2"),
            os.getenv("GEMINI_API_KEY3"),
            os.getenv("GEMINI_API_KEY4"),
            os.getenv("GEMINI_API_KEY5"),
        ]
        # Filtrar None/empty
        self.api_keys: List[str] = [k for k in raw_keys if k and k.strip()]
        if not self.api_keys:
            raise ValueError("No se encontraron GEMINI_API_KEY1..GEMINI_API_KEY5 en el entorno")

        self._key_index = 0  # índice de rotación
        logger.info(f"🔑 {len(self.api_keys)} Gemini API key(s) cargadas para rotación")

        # Redis
        self.redis_host = os.getenv('REDIS_HOST', 'cache')
        self.redis_port = int(os.getenv('REDIS_PORT', 6379))
        self.redis_client = redis.Redis(host=self.redis_host, port=self.redis_port, decode_responses=True)

        # Base de datos
        self.db_config = {
            'host': os.getenv('DB_HOST', 'database'),
            'database': os.getenv('DB_NAME', 'yahoo_qa'),
            'user': os.getenv('DB_USER', 'postgres'),
            'password': os.getenv('DB_PASSWORD', 'password123'),
            'port': os.getenv('DB_PORT', '5432')
        }
        self.db_connection = self._connect_to_db()
        logger.info("✅ LLM Client inicializado correctamente")

    # ---------------------------
    # Database helpers
    # ---------------------------
    def _connect_to_db(self):
        for attempt in range(5):
            try:
                conn = psycopg2.connect(**self.db_config)
                logger.info("✅ Conectado a la base de datos")
                return conn
            except Exception as e:
                logger.warning(f"Intento {attempt+1}/5 fallo al conectar BD: {e}")
                time.sleep(5)
        raise Exception("No se pudo conectar a la base de datos")

    def export_db_to_json(self, file_path: str = "/data/questions_backup.json"):
        """
        Exporta la tabla `questions` a JSON. Si el archivo ya existe, NO lo sobrescribe;
        en su lugar informa cuántas preguntas contiene.
        """
        # Si ya existe, mostrar cantidad y no recrear
        if os.path.exists(file_path):
            try:
                with open(file_path, "r", encoding="utf-8") as f:
                    data = json.load(f)
                logger.info(f"📄 JSON ya existe: {len(data)} preguntas presentes")
                return
            except Exception:
                logger.warning("⚠ JSON existente corrupto o ilegible — se sobrescribirá")

        try:
            cur = self.db_connection.cursor()
            cur.execute("SELECT id, question_text, human_answer, llm_answer, created_at, evaluated_at FROM questions")
            rows = cur.fetchall()
            cur.close()

            data = []
            for r in rows:
                data.append({
                    "id": r[0],
                    "question_text": r[1],
                    "human_answer": r[2],
                    "llm_answer": r[3],
                    "created_at": r[4].isoformat() if r[4] else None,
                    "evaluated_at": r[5].isoformat() if r[5] else None
                })

            with open(file_path, "w", encoding="utf-8") as f:
                json.dump(data, f, indent=2, ensure_ascii=False)

            logger.info(f"✅ Base de datos exportada a JSON: {file_path} ({len(data)} preguntas)")
        except Exception as e:
            logger.error(f"❌ Error exportando DB a JSON: {e}")

    # ---------------------------
    # Cache helpers
    # ---------------------------
    def _generate_cache_key(self, question: str) -> str:
        return f"llm_answer:{hash(question.strip().lower())}"

    def get_cached_answer(self, question: str) -> Optional[str]:
        return self.redis_client.get(self._generate_cache_key(question))

    def cache_answer(self, question: str, answer: str, ttl: int = 3600):
        try:
            self.redis_client.setex(self._generate_cache_key(question), ttl, answer)
        except Exception as e:
            logger.warning(f"⚠ No se pudo escribir en Redis: {e}")

    # ---------------------------
    # DB question helpers
    # ---------------------------
    def get_answer_from_db(self, question: str) -> Optional[str]:
        cur = self.db_connection.cursor()
        cur.execute(
            "SELECT llm_answer FROM questions WHERE question_text ILIKE %s AND llm_answer IS NOT NULL LIMIT 1",
            (f"%{question}%",)
        )
        row = cur.fetchone()
        cur.close()
        return row[0] if row else None

    def save_question_answer_to_db(self, question: str, llm_answer: str, human_answer: str = ""):
        """Guarda o actualiza pregunta; evita NOT NULL error guardando '' si human_answer es None."""
        cur = self.db_connection.cursor()
        cur.execute("SELECT id FROM questions WHERE question_text=%s", (question,))
        existing = cur.fetchone()
        if existing:
            cur.execute(
                "UPDATE questions SET llm_answer=%s, human_answer=COALESCE(%s, human_answer) WHERE question_text=%s",
                (llm_answer, human_answer or "", question)
            )
        else:
            cur.execute(
                "INSERT INTO questions (question_text, human_answer, llm_answer) VALUES (%s,%s,%s)",
                (question, human_answer or "", llm_answer)
            )
        self.db_connection.commit()
        cur.close()

    def get_next_questions(self, limit: int = 30):
        cur = self.db_connection.cursor()
        cur.execute("SELECT question_text FROM questions WHERE llm_answer IS NULL LIMIT %s", (limit,))
        rows = cur.fetchall()
        cur.close()
        return [r[0] for r in rows]

    # ---------------------------
    # Gemini API (rotación de claves)
    # ---------------------------
    def _current_api_key(self) -> str:
        return self.api_keys[self._key_index]

    def _rotate_key(self):
        """Avanza al siguiente índice de clave (round-robin)."""
        self._key_index = (self._key_index + 1) % len(self.api_keys)
        logger.info(f"🔁 Rotando a la siguiente API key (index={self._key_index})")

    def _call_gemini_once(self, prompt: str, api_key: str, timeout: int = 30):
        """Hace un POST a Gemini con la api_key proporcionada; devuelve (status_code, json/None, text)."""
        headers = {
            "Content-Type": "application/json",
            "X-Goog-Api-Key": api_key
        }
        payload = {
            "contents": [
                {"parts": [{"text": prompt}]}
            ]
        }
        try:
            resp = requests.post(GEMINI_URL, headers=headers, json=payload, timeout=timeout)
            try:
                return resp.status_code, resp.json(), resp.text
            except ValueError:
                return resp.status_code, None, resp.text
        except requests.RequestException as e:
            return None, None, str(e)

    def get_answer_from_gemini(self, question: str, max_key_attempts: int = None) -> str:
        """
        Llama a Gemini rotando entre keys. Devuelve texto o mensaje de error.
        max_key_attempts: si None, intentará tantas keys como haya.
        """
        prompt = f"Eres un experto en finanzas. Responde concisamente:\nPregunta: {question}\nRespuesta:"
        # Cuántas keys probaremos por batch
        keys_to_try = max_key_attempts if max_key_attempts is not None else len(self.api_keys)
        tried = 0
        start_index = self._key_index

        while tried < keys_to_try:
            api_key = self._current_api_key()
            status, j, raw_text = self._call_gemini_once(prompt, api_key)
            if status == 200 and j is not None:
                # Re-intentos de parseo seguro: v1beta puede devolver diferentes shapes.
                # Soportamos tanto "candidates" como "candidates[0].content.parts[0].text"
                try:
                    # primero intentar forma 'candidates'
                    cand = j.get("candidates")
                    if cand and isinstance(cand, list):
                        text = cand[0].get("content", {}).get("parts", [])[0].get("text", "")
                        if text:
                            return text.strip()
                    # fallback a 'output' o otras formas
                    # some responses might have 'output' or nested fields
                    if "output" in j:
                        if isinstance(j["output"], str) and j["output"].strip():
                            return j["output"].strip()
                    # fallback: buscar texto en raw_text
                    if raw_text:
                        # intentar extraer candidate text crudamente
                        import re
                        m = re.search(r'"text"\s*:\s*"([^"]{1,2000})"', raw_text)
                        if m:
                            return m.group(1).encode().decode('unicode_escape').strip()
                except Exception:
                    pass

                # si no se pudo parsear, devolvemos raw_text o mensaje útil
                return raw_text.strip() if raw_text else "Respuesta recibida pero no pudo parsearse."

            # Si status es None => excepción de requests, rotamos
            if status is None:
                logger.warning(f"⚠ Error de conexión usando key index {self._key_index}: {raw_text}")
                self._rotate_key()
                tried += 1
                continue

            # Si recibimos 429/503/500/403 intentamos con la siguiente key
            if status in (429, 503, 500, 403):
                logger.warning(f"⚠ Gemini responded {status} with key index {self._key_index}. Rotando key.")
                self._rotate_key()
                tried += 1
                continue

            # Errores de formato (400 etc) o no autorizados: registrar y rotar si posible
            logger.error(f"❌ Error Gemini API (status {status}): {raw_text}")
            # Rotar y probar con la siguiente hasta keys_to_try
            self._rotate_key()
            tried += 1

        # Si llegamos aquí, todas las keys / reintentos fallaron
        return f"Error al consultar Gemini: todas las keys probaron y fallaron (último estado {status})"

    # ---------------------------
    # Batch processing
    # ---------------------------
    def run_batches(self, batch_size: int = 30, wait_seconds: int = 60, max_questions: int = 150000):
        """
        Procesa batches de preguntas no respondidas, guarda JSON después de cada batch,
        rota keys en la API y muestra progreso en formato X/30000.
        """
        # total en tabla
        cur = self.db_connection.cursor()
        cur.execute("SELECT COUNT(*) FROM questions")
        total_in_table = cur.fetchone()[0]
        cur.execute("SELECT COUNT(*) FROM questions WHERE llm_answer IS NOT NULL")
        already_answered = cur.fetchone()[0]
        cur.close()

        # processed_count inicia con las ya respondidas (permite resume)
        processed_count = already_answered
        logger.info(f"🔁 Empezando procesamiento. Ya respondidas: {already_answered}/{total_in_table}")

        # Mientras no superemos el límite máximo global
        while processed_count < max_questions:
            questions = self.get_next_questions(limit=batch_size)
            if not questions:
                logger.info("✅ No quedan preguntas sin respuesta en la BD")
                break

            for q in questions:
                if processed_count >= max_questions:
                    break

                result = None
                try:
                    # Process flow: cache -> db -> gemini
                    # 1) cache
                    cached = self.get_cached_answer(q)
                    if cached:
                        answer = cached
                        source = "cache"
                    else:
                        # 2) check DB for similar
                        db_ans = self.get_answer_from_db(q)
                        if db_ans:
                            answer = db_ans
                            source = "database"
                            # cache it
                            self.cache_answer(q, answer)
                        else:
                            # 3) call Gemini (rotating keys automatically)
                            answer = self.get_answer_from_gemini(q)
                            source = "gemini"
                            # save and cache
                            self.save_question_answer_to_db(q, answer)
                            self.cache_answer(q, answer)

                    processed_count += 1
                    # Mostrar progreso relativo a TARGET_TOTAL (ej: X/30000)
                    display_total = min(TARGET_TOTAL, total_in_table)
                    logger.info(f"📊 Progreso: {processed_count}/{TARGET_TOTAL} (target) — actual en tabla: {processed_count}/{display_total}")
                    result = {
                        "question": q,
                        "answer": answer,
                        "source": source,
                        "timestamp": datetime.now().isoformat()
                    }
                    print(result)

                except Exception as e:
                    logger.error(f"❌ Error procesando pregunta: {e}")

            # guardar JSON de respaldo (no sobrescribe si ya existe)
            self.export_db_to_json("/data/questions_backup.json")

            if processed_count >= max_questions:
                logger.info(f"🔚 Límite de procesamiento alcanzado: {processed_count}/{max_questions}")
                break

            logger.info(f"⏱ Esperando {wait_seconds} segundos antes del siguiente batch...")
            time.sleep(wait_seconds)

        logger.info("🏁 Run_batches finalizado")

# ---------------------------
# Main
# ---------------------------
def main():
    client = LLMClient()
    # Lanza batches (configura batch_size / wait_seconds / max_questions si quieres)
    client.run_batches(batch_size=30, wait_seconds=60, max_questions=150000)

if __name__ == "__main__":
    main()
