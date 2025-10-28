# app.py
import os
import logging
from datetime import datetime
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
import psycopg2
from openai import OpenAI
import json

# --- Logging ---
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# --- FastAPI ---
app = FastAPI(title="Evaluador LLM por ID")

# --- Pydantic model ---
class QuestionRequest(BaseModel):
    id: int  # ahora recibe solo la ID

# --- DB config ---
DB_CONFIG = {
    "host": os.getenv("DB_HOST", "database"),
    "port": int(os.getenv("DB_PORT", 5432)),
    "database": os.getenv("DB_NAME", "yahoo_qa"),
    "user": os.getenv("DB_USER", "postgres"),
    "password": os.getenv("DB_PASSWORD", "password123")
}

# --- LLM helper ---
def call_llm(prompt: str, api_key: str, model="openai/gpt-oss-20b:free"):
    try:
        client = OpenAI(base_url="https://openrouter.ai/api/v1", api_key=api_key)
        completion = client.chat.completions.create(
            model=model,
            messages=[{"role": "user", "content": prompt}],
            extra_headers={
                "HTTP-Referer": "https://tu-sistema.local",
                "X-Title": "Evaluador automático"
            },
            timeout=60
        )
        return completion.choices[0].message.content.strip()
    except Exception as e:
        logger.exception("Error llamando al modelo LLM")
        raise HTTPException(status_code=500, detail=f"Error LLM: {e}")

# --- Evaluación con LLM ---
def evaluate_response_with_llm(human_answer, llm_answer, api_key):
    prompt = f"""
Evalúa la respuesta HUMANA comparada con la respuesta del MODELO.

### Respuesta humana
{human_answer}

### Respuesta del modelo
{llm_answer}

### Instrucciones
Devuelve un JSON con estos campos entre 0 y 1:
- similarity_score
- quality_score
- completeness_score
"""
    result_text = call_llm(prompt, api_key)
    try:
        data = json.loads(result_text)
        sim = float(data.get("similarity_score", 0.0))
        qual = float(data.get("quality_score", 0.0))
        comp = float(data.get("completeness_score", 0.0))
    except Exception:
        sim, qual, comp = 0.0, 0.0, 0.0
    return sim, qual, comp

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

# --- Endpoint principal ---
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

    # 2️⃣ Llamar al LLM para generar respuesta
    llm_answer = call_llm(question_text, key_to_use)

    # 3️⃣ Evaluar con LLM
    sim, qual, comp = evaluate_response_with_llm(human_answer, llm_answer, key_to_use)
    overall = round(sim * 0.5 + qual * 0.3 + comp * 0.2, 6)

    # 4️⃣ Guardar en DB
    eval_id = None
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO evaluations (question_text, human_answer, llm_answer,
                                         similarity_score, quality_score,
                                         completeness_score, overall_score, evaluated_at)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
                RETURNING id
            """, (question_text, human_answer, llm_answer,
                  sim, qual, comp, overall, datetime.utcnow()))
            eval_id = cur.fetchone()[0]
            conn.commit()
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
