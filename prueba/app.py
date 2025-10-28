import os
import logging
import json
import threading
import time
from datetime import datetime
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
import psycopg2
from openai import OpenAI
from confluent_kafka import Consumer, Producer

# --- Logging ---
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# --- FastAPI ---
app = FastAPI(title="Evaluador LLM por ID")

# --- Pydantic model ---
class QuestionRequest(BaseModel):
    id: int

# --- DB config ---
DB_CONFIG = {
    "host": os.getenv("DB_HOST", "database"),
    "port": int(os.getenv("DB_PORT", 5432)),
    "database": os.getenv("DB_NAME", "yahoo_qa"),
    "user": os.getenv("DB_USER", "postgres"),
    "password": os.getenv("DB_PASSWORD", "password123")
}

# --- LLM helper ---
def call_llm(prompt: str, api_key: str, model="minimax/minimax-m2:free"):
    try:
        client = OpenAI(base_url="https://openrouter.ai/api/v1", api_key=api_key)
        completion = client.chat.completions.create(
            model=model,
            messages=[{"role": "user", "content": prompt}],
            timeout=60
        )
        return completion.choices[0].message.content.strip()
    except Exception as e:
        logger.exception("Error llamando al modelo LLM")
        raise HTTPException(status_code=500, detail=f"Error LLM: {e}")

# --- Generar respuesta LLM concisa ---
def generate_llm_answer(question_text, api_key):
    prompt = f"""
Responde a la pregunta de manera **concisa y directa**.
No escribas explicaciones largas si no son necesarias.

Pregunta:
{question_text}
"""
    return call_llm(prompt, api_key)

# --- Evaluación del modelo respecto a la respuesta humana ---
def evaluate_response_with_llm(llm_answer, human_answer, api_key):
    prompt = f"""
Evalúa la respuesta del MODELO comparada con la respuesta HUMANA.

### Respuesta del modelo
{llm_answer}

### Respuesta humana
{human_answer}

### Instrucciones
Devuelve un JSON con estos campos entre 0 y 1:
- similarity_score
- quality_score
- completeness_score

Solo devuelve el JSON, sin explicaciones.
"""
    result_text = call_llm(prompt, api_key)
    try:
        data = json.loads(result_text)
        sim = float(data.get("similarity_score", 0.5))
        qual = float(data.get("quality_score", 0.5))
        comp = float(data.get("completeness_score", 0.5))
    except Exception:
        sim, qual, comp = 0.5, 0.5, 0.5

    overall = round(sim * 0.5 + qual * 0.3 + comp * 0.2, 6)
    return sim, qual, comp, overall

# --- Guardar JSON local ---
def save_response_json(data: dict, filename="responses.json"):
    base_path = os.path.dirname(os.path.abspath(__file__))
    path = os.path.join(base_path, filename)
    try:
        if os.path.exists(path):
            with open(path, "r", encoding="utf-8") as f:
                all_data = json.load(f)
        else:
            all_data = []
        all_data.append(data)
        with open(path, "w", encoding="utf-8") as f:
            json.dump(all_data, f, ensure_ascii=False, indent=2)
    except Exception as e:
        logger.warning(f"No se pudo guardar respuesta en JSON: {e}")

# --- Endpoint principal HTTP ---
@app.post("/evaluate")
def evaluate_question(req: QuestionRequest):
    api_keys = [k.strip() for k in os.getenv("OPENROUTER_API_KEY", "").split(",") if k.strip()]
    if not api_keys:
        raise HTTPException(status_code=500, detail="No se encontraron API keys")
    key_to_use = api_keys[0]

    # 1️⃣ Obtener pregunta y respuesta humana de DB
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        with conn.cursor() as cur:
            cur.execute("SELECT question_text, human_answer FROM questions WHERE id=%s", (req.id,))
            row = cur.fetchone()
            if not row:
                raise HTTPException(status_code=404, detail=f"No se encontró la pregunta con id {req.id}")
            question_text, human_answer = row
    except Exception as e:
        logger.exception("Error consultando DB")
        raise HTTPException(status_code=500, detail=f"Error DB: {e}")
    finally:
        if 'conn' in locals():
            conn.close()

    # 2️⃣ Generar respuesta LLM concisa
    llm_answer = generate_llm_answer(question_text, key_to_use)

    # 3️⃣ Evaluar modelo respecto a humano
    sim, qual, comp, overall = evaluate_response_with_llm(llm_answer, human_answer, key_to_use)

    # 4️⃣ Guardar en DB
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        with conn.cursor() as cur:
            cur.execute("""
                UPDATE questions
                SET llm_answer=%s,
                    similarity_score=%s,
                    quality_score=%s,
                    completeness_score=%s,
                    overall_score=%s,
                    evaluated_at=%s
                WHERE id=%s
            """, (llm_answer, sim, qual, comp, overall, datetime.utcnow(), req.id))
            conn.commit()
            eval_id = req.id
    finally:
        if 'conn' in locals():
            conn.close()

    # 5️⃣ Guardar JSON local
    response_data = {
        "id": eval_id,
        "question_id": req.id,
        "question_text": question_text,
        "human_answer": human_answer,
        "llm_answer": llm_answer,
        "similarity_score": sim,
        "quality_score": qual,
        "completeness_score": comp,
        "overall_score": overall,
        "evaluated_at": datetime.utcnow().isoformat()
    }
    save_response_json(response_data)

    return response_data


# --- Kafka Consumer/Producer setup ---
KAFKA_BROKER = os.getenv("KAFKA_BROKER", "kafka:9092")
TOPIC_PREGUNTAS = "preguntas"
TOPIC_RESPUESTAS_OK = "respuestas_exitosas"
TOPIC_RESPUESTAS_FAIL = "respuestas_fallidas"

producer = Producer({'bootstrap.servers': KAFKA_BROKER})

consumer = Consumer({
    'bootstrap.servers': KAFKA_BROKER,
    'group.id': 'api_client_group',
    'auto.offset.reset': 'earliest'
})

MAX_RETRIES = 3

def kafka_worker():
    consumer.subscribe([TOPIC_PREGUNTAS])
    logger.info(f"🎧 Kafka consumer escuchando el tópico '{TOPIC_PREGUNTAS}'...")

    while True:
        msg = consumer.poll(1.0)
        if msg is None:
            continue
        if msg.error():
            logger.error(f"Error en consumo: {msg.error()}")
            continue

        try:
            payload = json.loads(msg.value().decode('utf-8'))
            qid = payload["id"]
            retries = payload.get("retries", 0)

            result = evaluate_question(QuestionRequest(id=qid))

            if result["overall_score"] >= 0.7:
                producer.produce(TOPIC_RESPUESTAS_OK, json.dumps(result).encode('utf-8'))
            else:
                if retries < MAX_RETRIES:
                    payload["retries"] = retries + 1
                    producer.produce(TOPIC_PREGUNTAS, json.dumps(payload).encode('utf-8'))
                    logger.info(f"🔁 Reintentando pregunta {qid} (intento {payload['retries']})")
                else:
                    producer.produce(TOPIC_RESPUESTAS_FAIL, json.dumps(result).encode('utf-8'))
                    logger.info(f"❌ Pregunta {qid} descartada tras {MAX_RETRIES} intentos")

            producer.flush()

        except Exception as e:
            logger.error(f"❌ Error procesando mensaje de Kafka: {e}")


# --- Ejecutar consumidor Kafka en segundo plano ---
@app.on_event("startup")
def start_kafka_background_thread():
    thread = threading.Thread(target=kafka_worker, daemon=True)
    thread.start()
    logger.info("🚀 Hilo de Kafka iniciado en segundo plano")
