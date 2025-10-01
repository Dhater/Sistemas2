import os
import requests
import json
import time
from itertools import cycle
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock

API_KEYS = [k.strip() for k in os.getenv("OPENROUTER_API_KEY", "").split(",") if k.strip()]
if not API_KEYS:
    raise ValueError("No se encontraron API keys en OPENROUTER_API_KEY")
api_keys_cycle = cycle(API_KEYS)

DATA_PATH = "/data/grok_answers.json"
OUTPUT_PATH = "/data/grok_answers_evaluated.json"

lock = Lock()

def call_grok(prompt, wait_on_fail=5):
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
            resp = requests.post(
                "https://openrouter.ai/api/v1/chat/completions",
                headers={"Authorization": f"Bearer {key}", "Content-Type": "application/json"},
                data=json.dumps(payload),
                timeout=30
            )
            resp.raise_for_status()
            data = resp.json()
            content = data["choices"][0]["message"]["content"]
            if isinstance(content, list):
                return " ".join([c.get("text", "") for c in content])
            return content
        except Exception as e:
            print(f"Error con key {key}: {e}. Rotando...")
        time.sleep(wait_on_fail)
    print("❌ Todas las API keys fallaron. Reintentando...")
    time.sleep(wait_on_fail*2)
    return call_grok(prompt, wait_on_fail)

def evaluate_response(human_answer, llm_answer):
    prompt = f"""
Evalúa estas respuestas:
Humana: {human_answer}
LLM: {llm_answer}

Responde en JSON:
{{
  "similarity_score": ...,
  "quality_score": ...,
  "completeness_score": ...
}}
"""
    try:
        response = call_grok(prompt)
        return json.loads(response)
    except Exception as e:
        print(f"❌ Error parseando JSON de evaluación: {e}")
        return {"similarity_score": 0, "quality_score": 0, "completeness_score": 0}

def calculate_overall(sim, qual, comp):
    return round(sim*0.5 + qual*0.3 + comp*0.2, 4)

def process_question(key, entry, evaluated_data):
    print(f"Evaluando pregunta {key}: {entry['question_text'][:50]}...")
    scores = evaluate_response(entry["human_answer"], entry["llm_answer"])
    overall = calculate_overall(
        scores.get("similarity_score", 0),
        scores.get("quality_score", 0),
        scores.get("completeness_score", 0)
    )

    entry["similarity_score"] = scores.get("similarity_score", 0)
    entry["quality_score"] = scores.get("quality_score", 0)
    entry["completeness_score"] = scores.get("completeness_score", 0)
    entry["overall_score"] = overall
    entry["evaluated_at"] = datetime.now().isoformat()

    with lock:
        evaluated_data[key] = entry
        with open(OUTPUT_PATH, "w", encoding="utf-8") as f:
            json.dump(evaluated_data, f, ensure_ascii=False, indent=2)

    print(f"✅ Pregunta {key} evaluada y guardada en {OUTPUT_PATH}.")

if __name__ == "__main__":
    if not os.path.exists(DATA_PATH):
        print(f"No se encontró el JSON en {DATA_PATH}")
        exit()

    with open(DATA_PATH, "r", encoding="utf-8") as f:
        data = json.load(f)

    evaluated_data = {}
    start_from = 0

    if os.path.exists(OUTPUT_PATH):
        with open(OUTPUT_PATH, "r", encoding="utf-8") as f:
            evaluated_data = json.load(f)
        if evaluated_data:
            # Tomamos la máxima key numérica para empezar desde la siguiente
            numeric_keys = [int(k) for k in evaluated_data.keys() if k.isdigit()]
            start_from = max(numeric_keys) + 1
        print(f"🔄 Reanudando desde la pregunta número {start_from}...")

    # Filtramos las preguntas pendientes
    pending_items = {k:v for k,v in data.items() if int(k) >= start_from}

    with ThreadPoolExecutor(max_workers=5) as executor:
        futures = [executor.submit(process_question, k, v, evaluated_data) for k,v in pending_items.items()]
        for _ in as_completed(futures):
            pass

    print("✅ Todas las preguntas evaluadas y guardadas en el nuevo JSON.")
