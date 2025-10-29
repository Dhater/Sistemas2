import os
import logging
import json
import threading
from datetime import datetime
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
import psycopg2
from openai import OpenAI
from confluent_kafka import Consumer, Producer
from zoneinfo import ZoneInfo

# --- Helper para datetime chileno ---
def get_chile_time():
    return datetime.now(ZoneInfo("America/Santiago"))

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

def generate_llm_answer(question_text, api_key):
    prompt = f"Responde de manera concisa:\n{question_text}"
    return call_llm(prompt, api_key)

def evaluate_response_with_llm(llm_answer, human_answer, api_key):
    prompt = f"""
Evalúa la respuesta del MODELO comparada con la HUMANA.
### Modelo
{llm_answer}
### Humana
{human_answer}
Devuelve un JSON: similarity_score, quality_score, completeness_score
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
        logger.info(f"✅ Guardada respuesta success id={data['question_id']}")
    except Exception as e:
        logger.warning(f"No se pudo guardar JSON: {e}")

# --- Endpoint HTTP ---
@app.post("/evaluate")
def evaluate_question(req: QuestionRequest):
    api_keys = [k.strip() for k in os.getenv("OPENROUTER_API_KEY", "").split(",") if k.strip()]
    if not api_keys:
        raise HTTPException(status_code=500, detail="No se encontraron API keys")
    key_to_use = api_keys[0]

    try:
        conn = psycopg2.connect(**DB_CONFIG)
        with conn.cursor() as cur:
            cur.execute("""
                SELECT question_text, human_answer, llm_answer,
                    similarity_score, quality_score, completeness_score, overall_score
                FROM questions WHERE id=%s
            """, (req.id,))
            row = cur.fetchone()
            if not row:
                raise HTTPException(status_code=404, detail=f"No se encontró la pregunta con id {req.id}")
            question_text, human_answer, existing_llm_answer, existing_sim, existing_qual, existing_comp, existing_overall = row

            if existing_llm_answer and existing_overall and existing_overall >= 0.7:
                logger.info(f"♻️ Reutilizando respuesta previa id={req.id}")
                response_data = {
                    "id": req.id,
                    "question_id": req.id,
                    "question_text": question_text,
                    "human_answer": human_answer,
                    "llm_answer": existing_llm_answer,
                    "similarity_score": existing_sim,
                    "quality_score": existing_qual,
                    "completeness_score": existing_comp,
                    "overall_score": existing_overall,
                    "evaluated_at": get_chile_time().isoformat()
                }
                save_response_json(response_data)
                return response_data
    finally:
        if 'conn' in locals():
            conn.close()

    llm_answer = generate_llm_answer(question_text, key_to_use)
    sim, qual, comp, overall = evaluate_response_with_llm(llm_answer, human_answer, key_to_use)
    response_data = {
        "id": req.id,
        "question_id": req.id,
        "question_text": question_text,
        "human_answer": human_answer,
        "llm_answer": llm_answer,
        "similarity_score": sim,
        "quality_score": qual,
        "completeness_score": comp,
        "overall_score": overall,
        "evaluated_at": get_chile_time().isoformat()
    }

    if overall >= 0.7:
        try:
            conn = psycopg2.connect(**DB_CONFIG)
            with conn.cursor() as cur:
                cur.execute("""
                    UPDATE questions
                    SET llm_answer=%s, similarity_score=%s, quality_score=%s, completeness_score=%s,
                        overall_score=%s, evaluated_at=%s
                    WHERE id=%s
                """, (llm_answer, sim, qual, comp, overall, get_chile_time(), req.id))
                conn.commit()
        finally:
            if 'conn' in locals():
                conn.close()
        save_response_json(response_data)
        logger.info(f"✅ Guardada respuesta id={req.id}")
    return response_data
