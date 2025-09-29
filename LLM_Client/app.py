import os
import time
import requests
import psycopg2
import redis
import json
import logging
import re
from datetime import datetime
from dotenv import load_dotenv

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

load_dotenv()

GEMINI_URL = "https://generativelanguage.googleapis.com/v1beta/models/gemini-2.0-flash:generateContent"

class LLMClient:
    def __init__(self):
        self.gemini_api_key = os.getenv('GEMINI_API_KEY')
        if not self.gemini_api_key:
            raise ValueError("GEMINI_API_KEY no encontrada")

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

    def _connect_to_db(self):
        for attempt in range(5):
            try:
                conn = psycopg2.connect(**self.db_config)
                logger.info("✅ Conectado a la base de datos")
                return conn
            except Exception as e:
                logger.warning(f"Intento {attempt+1}/5 fallo: {e}")
                time.sleep(5)
        raise Exception("No se pudo conectar a la base de datos")

    def export_db_to_json(self, file_path: str = "/data/questions_backup.json"):
        # Si el JSON ya existe, solo mostrar la cantidad de preguntas
        if os.path.exists(file_path):
            with open(file_path, "r", encoding="utf-8") as f:
                try:
                    data = json.load(f)
                    logger.info(f"📄 JSON ya existe: {len(data)} preguntas presentes")
                    return
                except Exception:
                    logger.warning("⚠ JSON existente corrupto, se sobrescribirá")

        try:
            cursor = self.db_connection.cursor()
            cursor.execute("SELECT id, question_text, human_answer, llm_answer, created_at, evaluated_at FROM questions")
            rows = cursor.fetchall()
            cursor.close()
            data = []
            for row in rows:
                data.append({
                    "id": row[0],
                    "question_text": row[1],
                    "human_answer": row[2],
                    "llm_answer": row[3],
                    "created_at": row[4].isoformat() if row[4] else None,
                    "evaluated_at": row[5].isoformat() if row[5] else None
                })
            with open(file_path, "w", encoding="utf-8") as f:
                json.dump(data, f, indent=2, ensure_ascii=False)
            logger.info(f"✅ Base de datos exportada a JSON: {file_path} ({len(data)} preguntas)")
        except Exception as e:
            logger.error(f"❌ Error exportando DB a JSON: {e}")

    def _generate_cache_key(self, question: str):
        return f"llm_answer:{hash(question.strip().lower())}"

    def get_cached_answer(self, question: str):
        return self.redis_client.get(self._generate_cache_key(question))

    def cache_answer(self, question: str, answer: str, ttl: int = 3600):
        self.redis_client.setex(self._generate_cache_key(question), ttl, answer)

    def get_answer_from_db(self, question: str):
        cursor = self.db_connection.cursor()
        cursor.execute(
            "SELECT llm_answer FROM questions WHERE question_text ILIKE %s AND llm_answer IS NOT NULL LIMIT 1",
            (f"%{question}%",)
        )
        result = cursor.fetchone()
        cursor.close()
        return result[0] if result else None

    def save_question_answer_to_db(self, question: str, llm_answer: str, human_answer: str = ""):
        cursor = self.db_connection.cursor()
        cursor.execute("SELECT id FROM questions WHERE question_text=%s", (question,))
        existing = cursor.fetchone()
        if existing:
            cursor.execute(
                "UPDATE questions SET llm_answer=%s, human_answer=COALESCE(%s, human_answer) WHERE question_text=%s",
                (llm_answer, human_answer or "", question)
            )
        else:
            cursor.execute(
                "INSERT INTO questions (question_text, human_answer, llm_answer) VALUES (%s,%s,%s)",
                (question, human_answer or "", llm_answer)
            )
        self.db_connection.commit()
        cursor.close()

    def get_answer_from_gemini(self, question: str):
        try:
            headers = {
                "Content-Type": "application/json",
                "X-Goog-Api-Key": self.gemini_api_key
            }
            prompt = f"Eres un experto en finanzas. Responde concisamente:\nPregunta: {question}\nRespuesta:"
            payload = {
                "contents": [
                    {"parts": [{"text": prompt}]}
                ]
            }
            response = requests.post(GEMINI_URL, headers=headers, json=payload)
            if response.status_code == 200:
                result = response.json()
                try:
                    text = result["candidates"][0]["content"]["parts"][0]["text"].strip()
                    return text
                except (KeyError, IndexError):
                    return "No se obtuvo respuesta de Gemini."
            else:
                logger.error(f"Error Gemini API: {response.status_code} {response.text}")
                return f"Error al consultar Gemini: {response.status_code}"
        except Exception as e:
            logger.error(e)
            return f"Error: {str(e)}"

    def process_question(self, question: str, use_cache=True, save_to_db=True):
        start_time = time.time()
        answer = None
        source = "unknown"

        if use_cache:
            answer = self.get_cached_answer(question)
            if answer:
                source = "cache"

        if not answer:
            answer = self.get_answer_from_db(question)
            if answer:
                source = "database"
                if use_cache:
                    self.cache_answer(question, answer)

        if not answer:
            answer = self.get_answer_from_gemini(question)
            source = "gemini"
            if save_to_db:
                self.save_question_answer_to_db(question, answer)
            if use_cache:
                self.cache_answer(question, answer)

        return {
            "question": question,
            "answer": answer,
            "source": source,
            "response_time": round(time.time() - start_time, 3),
            "timestamp": datetime.now().isoformat()
        }

    def get_next_questions(self, limit=20):
        cursor = self.db_connection.cursor()
        cursor.execute("SELECT question_text FROM questions WHERE llm_answer IS NULL LIMIT %s", (limit,))
        questions = [row[0] for row in cursor.fetchall()]
        cursor.close()
        return questions

    def run_batches(self, batch_size=20, wait_seconds=60, max_questions=150000):
        # Contador de preguntas procesadas
        cursor = self.db_connection.cursor()
        cursor.execute("SELECT COUNT(*) FROM questions")
        total_questions = cursor.fetchone()[0]
        cursor.close()

        processed_count = 0
        while processed_count < max_questions:
            questions = self.get_next_questions(limit=batch_size)
            if not questions:
                logger.info("✅ Todas las preguntas ya fueron procesadas")
                break

            for q in questions:
                if processed_count >= max_questions:
                    break  # Salir si llegamos al límite
                res = self.process_question(q)
                processed_count += 1
                print(res)
                logger.info(f"📊 Progreso: {processed_count}/{min(total_questions, max_questions)} preguntas procesadas")

            # Guardar respaldo después de cada batch
            self.export_db_to_json("/data/questions_backup.json")

            logger.info(f"⏱ Esperando {wait_seconds} segundos antes del siguiente batch...")
            time.sleep(wait_seconds)



def main():
    client = LLMClient()
    client.run_batches(batch_size=20, wait_seconds=60)
    logger.info("🚀 LLM Client finalizó todas las tiradas")


if __name__ == "__main__":
    main()
