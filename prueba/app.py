# app.py
import os
import logging
from datetime import datetime
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
import psycopg2
from openai import OpenAI
import json
import traceback
import time

# --- Logging ---
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# --- FastAPI ---
app = FastAPI(title="Prueba API Keys, DB y LLM")

# --- Pydantic request model ---
class QuestionRequest(BaseModel):
    question_text: str
    human_answer: str = ""  # opcional

# --- DB config ---
DB_CONFIG = {
    "host": os.getenv("DB_HOST", "database"),
    "port": int(os.getenv("DB_PORT", 5432)),
    "database": os.getenv("DB_NAME", "yahoo_qa"),
    "user": os.getenv("DB_USER", "postgres"),
    "password": os.getenv("DB_PASSWORD", "password123")
}

# --- LLM helper ---
def call_llm(prompt: str, api_key: str, max_words: int = 120):
    try:
        client = OpenAI(
            base_url="https://openrouter.ai/api/v1",
            api_key=api_key
        )

        full_prompt = f"{prompt}\n\nPlease answer in {max_words} words or less."

        completion = client.chat.completions.create(
            model="openai/gpt-oss-20b:free",
            messages=[{"role": "user", "content": full_prompt}],
            extra_headers={
                "HTTP-Referer": "https://tu-sistema.local",
                "X-Title": "Prueba LLM"
            },
            extra_body={},
            timeout=60
        )

        return completion.choices[0].message.content.strip()
    except Exception as e:
        logger.exception("Error llamando al modelo LLM")
        raise HTTPException(status_code=500, detail=f"Error LLM: {e}")

# --- Evaluación simple ---
def evaluate_response(human_answer, llm_answer):
    try:
        sim_score = 1.0 if human_answer.strip().lower() == llm_answer.strip().lower() else 0.5
        quality_score = 0.8
        completeness_score = 0.9
        return sim_score, quality_score, completeness_score
    except:
        return 0.0, 0.0, 0.0

# --- Guardar en JSON local en la misma carpeta que app.py ---
def save_response_json(data: dict, filename="responses.json"):
    try:
        base_path = os.path.dirname(os.path.abspath(__file__))
        path = os.path.join(base_path, filename)

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

# --- Endpoint POST para recibir pregunta, generar respuesta y guardar ---
@app.post("/evaluate")
def evaluate_question(req: QuestionRequest):
    # 1️⃣ Verificar API Keys
    api_keys = [k.strip() for k in os.getenv("OPENROUTER_API_KEY", "").split(",") if k.strip()]
    if not api_keys:
        logger.error("No se encontraron API keys")
        raise HTTPException(status_code=500, detail="API keys no encontradas")
    key_to_use = api_keys[0]

    # 2️⃣ Llamar al LLM
    logger.info(f"Pregunta recibida: {req.question_text[:60]}...")
    llm_answer = call_llm(req.question_text, key_to_use)
    logger.info(f"Respuesta LLM: {llm_answer[:100]}...")

    # 3️⃣ Evaluar respuesta
    sim, qual, comp = evaluate_response(req.human_answer, llm_answer)
    overall = round(sim * 0.5 + qual * 0.3 + comp * 0.2, 6)

    # 4️⃣ Guardar en DB
    eval_id = None
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO evaluations (question_text, human_answer, llm_answer,
                                         similarity_score, quality_score,
                                         completeness_score, overall_score,
                                         evaluated_at)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
                RETURNING id
            """, (req.question_text, req.human_answer, llm_answer,
                  sim, qual, comp, overall, datetime.utcnow()))
            eval_id = cur.fetchone()[0]
            conn.commit()
            logger.info(f"💾 Evaluación guardada en DB con id={eval_id}")
    except Exception as e:
        logger.exception("Error guardando evaluación en DB")
        raise HTTPException(status_code=500, detail=f"Error DB: {e}")
    finally:
        if 'conn' in locals():
            conn.close()

    # 5️⃣ Guardar también en JSON local
    response_data = {
        "id": eval_id,
        "question_text": req.question_text,
        "human_answer": req.human_answer,
        "llm_answer": llm_answer,
        "similarity_score": sim,
        "quality_score": qual,
        "completeness_score": comp,
        "overall_score": overall,
        "evaluated_at": datetime.utcnow().isoformat()
    }
    save_response_json(response_data)

    # 6️⃣ Devolver JSON con resultados
    return response_data
