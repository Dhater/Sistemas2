import os
import json
import requests
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from datetime import datetime
import psycopg2
from itertools import cycle
import time
import re

# --- Configuración API Keys ---
API_KEYS = [k.strip() for k in os.getenv("OPENROUTER_API_KEY", "").split(",") if k.strip()]
if not API_KEYS:
    raise ValueError("No se encontraron API keys en OPENROUTER_API_KEY")
api_keys_cycle = cycle(API_KEYS)

# --- Configuración DB ---
DB_CONFIG = {
    "host": os.getenv("DB_HOST", "database"),
    "port": int(os.getenv("DB_PORT", 5432)),
    "database": os.getenv("DB_NAME", "yahoo_qa"),
    "user": os.getenv("DB_USER", "postgres"),
    "password": os.getenv("DB_PASSWORD", "password123")
}

# --- FastAPI ---
app = FastAPI(title="Grok LLM Evaluator & Scorer")

class QuestionRequest(BaseModel):
    id: int

# --- Helpers ---
def safe_load_json_from_text(text):
    text = text.strip()
    if text.startswith("{") or text.startswith("["):
        try:
            return json.loads(text)
        except Exception:
            pass
    match = re.search(r"(\{(?:[^{}]|(?R))*\})", text, flags=re.DOTALL)
    if match:
        try:
            return json.loads(match.group(1))
        except Exception:
            pass
    return None

def call_grok(prompt, wait_on_fail=10):
    tried_keys = set()
    while len(tried_keys) < len(API_KEYS):
        key = next(api_keys_cycle)
        if key in tried_keys:
            continue
        tried_keys.add(key)

        payload = {
            "model": "x-ai/grok-4-fast:free",
            "messages": [{"role": "user", "content": [{"type": "text", "text": prompt}]}]
        }

        try:
            print(f"⏳ [Checkpoint] Llamando a Grok con key {key}...")
            resp = requests.post(
                "https://openrouter.ai/api/v1/chat/completions",
                headers={"Authorization": f"Bearer {key}", "Content-Type": "application/json"},
                json=payload,
                timeout=30
            )
            data = resp.json()
            if "choices" in data and data["choices"]:
                content = data["choices"][0]["message"]["content"]
                if isinstance(content, list):
                    result = " ".join([c.get("text", "") for c in content])
                else:
                    result = content
                print(f"✅ [Checkpoint] Respuesta recibida de Grok (key {key})")
                return result
        except Exception as e:
            print(f"❌ Error con key {key}: {e}. Rotando...")
        time.sleep(wait_on_fail)
    raise RuntimeError("Todas las API keys fallaron.")

# --- Scorer ---
def evaluate_response(human_answer, llm_answer):
    print("⏳ [Checkpoint] Evaluando respuesta con scorer...")
    prompt = f"""
Evalúa estas respuestas:
Humana: {human_answer}
LLM: {llm_answer}

Responde en JSON con exactamente estas claves:
{{
  "similarity_score": 0.0,
  "quality_score": 0.0,
  "completeness_score": 0.0
}}
Devuelve SOLO JSON (si puedes). Si no, incluye el JSON en alguna parte del texto.
"""
    try:
        raw = call_grok(prompt)
        parsed = safe_load_json_from_text(raw)
        if parsed is None:
            print("⚠️ No se pudo extraer JSON de la evaluación. Texto devuelto:", raw[:200])
            return {"similarity_score": 0.0, "quality_score": 0.0, "completeness_score": 0.0}
        def to_float(v):
            try:
                return float(v)
            except:
                return 0.0
        print("✅ [Checkpoint] Evaluación completada")
        return {
            "similarity_score": to_float(parsed.get("similarity_score", 0.0)),
            "quality_score": to_float(parsed.get("quality_score", 0.0)),
            "completeness_score": to_float(parsed.get("completeness_score", 0.0)),
        }
    except Exception as e:
        print(f"❌ Error en evaluate_response: {e}")
        return {"similarity_score": 0.0, "quality_score": 0.0, "completeness_score": 0.0}

def calculate_overall(sim, qual, comp):
    return round(sim * 0.5 + qual * 0.3 + comp * 0.2, 6)

# --- Endpoint ---
@app.post("/evaluate")
def evaluate_question(req: QuestionRequest):
    print(f"⏳ [Checkpoint] Recibida petición para id={req.id}")
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        with conn.cursor() as cur:
            print("⏳ [Checkpoint] Conectado a la DB, buscando pregunta...")
            cur.execute("SELECT id, question_text, human_answer FROM questions WHERE id=%s", (req.id,))
            row = cur.fetchone()
            if not row:
                print(f"❌ [Checkpoint] Pregunta con id={req.id} no encontrada")
                raise HTTPException(status_code=404, detail=f"No se encontró la pregunta con id {req.id}")
            qid, question_text, human_answer = row
            print(f"✅ [Checkpoint] Pregunta encontrada: {question_text[:50]}...")

            # 1️⃣ Generar llm_answer
            llm_answer = call_grok(question_text)

            # 2️⃣ Evaluar con scorer
            scores = evaluate_response(human_answer, llm_answer)
            sim = scores["similarity_score"]
            qual = scores["quality_score"]
            comp = scores["completeness_score"]
            overall = calculate_overall(sim, qual, comp)

            # 3️⃣ Actualizar DB
            print("⏳ [Checkpoint] Actualizando DB con resultados...")
            cur.execute("""
                UPDATE questions
                SET llm_answer=%s,
                    similarity_score=%s,
                    quality_score=%s,
                    completeness_score=%s,
                    overall_score=%s,
                    evaluated_at=%s
                WHERE id=%s
            """, (llm_answer, sim, qual, comp, overall, datetime.utcnow(), qid))
            conn.commit()
            print(f"✅ [Checkpoint] DB actualizada para id={qid}")

            # 4️⃣ Devolver JSON completo
            response = {
                "id": qid,
                "question_text": question_text,
                "human_answer": human_answer,
                "llm_answer": llm_answer,
                "similarity_score": sim,
                "quality_score": qual,
                "completeness_score": comp,
                "overall_score": overall,
                "evaluated_at": datetime.utcnow().isoformat()
            }
            print("✅ [Checkpoint] Respuesta enviada al cliente")
            return response

    except psycopg2.OperationalError as e:
        print(f"❌ [Checkpoint] Error de conexión a DB: {e}")
        raise HTTPException(status_code=500, detail=f"Error de conexión a DB: {e}")
    except RuntimeError as e:
        print(f"❌ [Checkpoint] RuntimeError: {e}")
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        if 'conn' in locals():
            conn.close()
            print("⏳ [Checkpoint] Conexión a DB cerrada")
