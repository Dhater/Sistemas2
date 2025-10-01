import os
import requests
import json
import psycopg2
from itertools import cycle
from datetime import datetime
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock
import shutil  # <-- para mover archivos
# --- Configuración API Keys ---
API_KEYS = [k.strip() for k in os.getenv("OPENROUTER_API_KEY", "").split(",") if k.strip()]
if not API_KEYS:
    raise ValueError("No se encontraron API keys en OPENROUTER_API_KEY")
api_keys_cycle = cycle(API_KEYS)  # ciclo infinito de keys

lock = Lock()  # Para acceso thread-safe a grok_answers

# --- Configuración DB ---
DB_CONFIG = {
    "host": os.getenv("DB_HOST", "database"),
    "port": int(os.getenv("DB_PORT", 5432)),
    "database": os.getenv("DB_NAME", "yahoo_qa"),
    "user": os.getenv("DB_USER", "postgres"),
    "password": os.getenv("DB_PASSWORD", "password123")
}

MAX_QUESTIONS = 15002  # Límite total de preguntas a procesar

# --- Función para llamar a Grok ---
def call_grok(question, image=None, wait_on_fail=10):
    tried_keys = set()
    while len(tried_keys) < len(API_KEYS):
        key = next(api_keys_cycle)
        if key in tried_keys:
            continue
        tried_keys.add(key)

        content_list = [{"type": "text", "text": question}]
        if image:
            content_list.append({"type": "image_url", "image_url": {"url": image}})

        payload = {
            "model": "x-ai/grok-4-fast:free",
            "messages": [{"role": "user", "content": content_list}]
        }

        try:
            response = requests.post(
                "https://openrouter.ai/api/v1/chat/completions",
                headers={"Authorization": f"Bearer {key}", "Content-Type": "application/json"},
                data=json.dumps(payload),
                timeout=30
            )
            data = response.json()
            if "choices" in data and data["choices"]:
                content = data["choices"][0]["message"]["content"]
                if isinstance(content, list):
                    return " ".join([c.get("text", "") for c in content])
                return content
        except requests.exceptions.RequestException as e:
            print(f"Error con key {key}: {e}. Rotando...")
        except json.JSONDecodeError:
            print(f"Key {key} devolvió respuesta no JSON. Rotando...")

        print(f"⏱ Esperando {wait_on_fail}s antes de intentar otra key...")
        time.sleep(wait_on_fail)

    print(f"❌ Todas las API keys fallaron. Esperando {wait_on_fail*2}s antes de reintentar...")
    time.sleep(wait_on_fail*2)
    return call_grok(question, image, wait_on_fail)

# --- Función worker ---
def process_question(q, total, idx):
    print(f"Trabajando pregunta {idx}/{total}: {q['question_text'][:50]}...")
    llm_answer = call_grok(q["question_text"], q.get("image_url"))
    result = {
        "question_text": q["question_text"],
        "human_answer": q.get("human_answer", ""),
        "llm_answer": llm_answer,
        "similarity_score": None,
        "quality_score": None,
        "completeness_score": None,
        "overall_score": None,
        "created_at": datetime.now().isoformat(),
        "evaluated_at": None
    }
    return q["id"], result

# --- Función principal ---
def main():
    base_path = os.path.dirname(os.path.abspath(__file__))

    # Rutas
    backup_json_path = os.path.join(base_path, "questions_backup.json")
    output_json_path = os.path.join(base_path, "grok_answers.json")

    # --- Cargar respuestas ya procesadas ---
    if os.path.exists(output_json_path):
        with open(output_json_path, "r", encoding="utf-8") as f:
            grok_answers = json.load(f)
        print(f"🔄 Continuando desde {len(grok_answers)} preguntas ya procesadas...")
    else:
        grok_answers = {}

    # --- Extraer preguntas de la DB ---
    questions = []
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        with conn.cursor() as cur:
            cur.execute("SELECT id, question_text, human_answer FROM questions ORDER BY id ASC")
            rows = cur.fetchall()
            for r in rows:
                questions.append({
                    "id": r[0],
                    "question_text": r[1],
                    "human_answer": r[2] or "",
                    "image_url": None
                })
    except psycopg2.OperationalError as e:
        print(f"❌ No se pudo conectar a la DB: {e}")
        return
    finally:
        if conn:
            conn.close()

    # --- Guardar backup de preguntas si no existe ---
    if not os.path.exists(backup_json_path):
        with open(backup_json_path, "w", encoding="utf-8") as f:
            json.dump(questions, f, ensure_ascii=False, indent=2)
        print(f"💾 Backup de preguntas guardado en {backup_json_path}")

    # --- Filtrar preguntas nuevas y limitar hasta MAX_QUESTIONS ---
    questions_to_process = [q for q in questions if str(q["id"]) not in grok_answers]
    already_processed = len(grok_answers)
    remaining_slots = MAX_QUESTIONS - already_processed
    if remaining_slots <= 0:
        print(f"✅ Ya se alcanzaron {MAX_QUESTIONS} preguntas procesadas.")
        return

    questions_to_process = questions_to_process[:remaining_slots]
    total = len(questions_to_process)
    print(f"📌 Total de preguntas nuevas a procesar en esta sesión: {total}")

    save_every = 1
    processed = 0

    with ThreadPoolExecutor(max_workers=10) as executor:
        futures = {executor.submit(process_question, q, total, i+1): i for i, q in enumerate(questions_to_process)}
        for future in as_completed(futures):
            qid, result = future.result()
            with lock:
                grok_answers[str(qid)] = result
                processed += 1
                if processed % save_every == 0:
                    with open(output_json_path, "w", encoding="utf-8") as f:
                        json.dump(grok_answers, f, ensure_ascii=False, indent=2)
                    print(f"💾 Guardadas {processed} preguntas nuevas hasta ahora...")

    # Guardar lo que quede al final
    with open(output_json_path, "w", encoding="utf-8") as f:
        json.dump(grok_answers, f, ensure_ascii=False, indent=2)
    print(f"✅ Grok answers guardadas en {output_json_path} (total procesadas en esta sesión: {processed})")

if __name__ == "__main__":
    main()