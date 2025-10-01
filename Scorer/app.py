# archivo: evaluate_grok_jsonl.py
import os
import requests
import json
import time
import re
from itertools import cycle
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock

# --- Configuración ---
API_KEYS = [k.strip() for k in os.getenv("OPENROUTER_API_KEY", "").split(",") if k.strip()]
if not API_KEYS:
    raise ValueError("No se encontraron API keys en OPENROUTER_API_KEY")
api_keys_cycle = cycle(API_KEYS)

DATA_PATH = "/data/grok_answers.json"            # input: JSON normal con todas las preguntas
OUTPUT_PATH = "/data/grok_answers_evaluated.jsonl"  # salida: JSONL (una entrada por línea)
TEMP_OUTPUT = OUTPUT_PATH + ".tmp"

lock = Lock()
session = requests.Session()
session.headers.update({"Content-Type": "application/json"})

# --- Helpers ---
def atomic_replace(src_path, dst_path):
    """Reemplaza dst_path por src_path de forma atómica (os.replace)."""
    os.replace(src_path, dst_path)

def safe_load_json_from_text(text):
    """
    Intenta extraer y parsear el primer objeto JSON válido dentro de `text`.
    Si falla, intenta limpiar y devolver None.
    """
    text = text.strip()
    # Si el texto empieza por '{' o '[' asumimos JSON directo
    if text.startswith("{") or text.startswith("["):
        try:
            return json.loads(text)
        except Exception:
            pass
    # Busca el primer bloque {...}
    match = re.search(r"(\{(?:[^{}]|(?R))*\})", text, flags=re.DOTALL)
    if match:
        try:
            return json.loads(match.group(1))
        except Exception:
            pass
    # no pudimos parsear
    return None

def call_grok(prompt, max_retries=3, base_wait=2):
    """
    Llamada no recursiva a la API; rota keys y hace backoff.
    Devuelve texto (tal cual) o lanza excepción si no hay respuesta.
    """
    tried = 0
    last_exc = None
    while tried < max_retries:
        key = next(api_keys_cycle)
        payload = {
            "model": "x-ai/grok-4-fast:free",
            "messages": [{"role": "user", "content": [{"type": "text", "text": prompt}]}]
        }
        try:
            resp = session.post(
                "https://openrouter.ai/api/v1/chat/completions",
                headers={"Authorization": f"Bearer {key}"},
                json=payload,
                timeout=60
            )
            resp.raise_for_status()
            data = resp.json()
            # Manejo flexible del contenido
            content = data["choices"][0]["message"]["content"]
            if isinstance(content, list):
                return " ".join([c.get("text", "") for c in content])
            return content
        except Exception as e:
            last_exc = e
            tried += 1
            wait = base_wait * tried
            print(f"Error con key {key}: {e}. Reintentando en {wait}s (intento {tried}/{max_retries})")
            time.sleep(wait)
    raise RuntimeError(f"Todas las reintentos fallaron: {last_exc}")

def evaluate_response(human_answer, llm_answer):
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
        # Aseguramos floats y límites 0..1 o 0..100 según tu convención; aquí asumimos 0..1
        def to_float(v):
            try:
                return float(v)
            except:
                return 0.0
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

# --- Worker para un thread ---
def process_question(key, entry, processed_keys):
    if key in processed_keys:
        # ya procesado
        return None

    print(f"Evaluando pregunta {key}...")
    scores = evaluate_response(entry.get("human_answer", ""), entry.get("llm_answer", ""))
    overall = calculate_overall(scores["similarity_score"], scores["quality_score"], scores["completeness_score"])

    entry["similarity_score"] = scores["similarity_score"]
    entry["quality_score"] = scores["quality_score"]
    entry["completeness_score"] = scores["completeness_score"]
    entry["overall_score"] = overall
    entry["evaluated_at"] = datetime.utcnow().isoformat()

    # Devolver la tupla para que el thread principal la escriba (append) sin necesidad de lock grueso
    return (key, entry)

# --- Main ---
def main():
    if not os.path.exists(DATA_PATH):
        print(f"No se encontró el JSON en {DATA_PATH}")
        return

    # carga el input completo (solo para iterar)
    with open(DATA_PATH, "r", encoding="utf-8") as f:
        data = json.load(f)

    # lee OUTPUT_PATH si existe (JSONL) para construir processed_keys
    processed_keys = set()
    if os.path.exists(OUTPUT_PATH):
        try:
            with open(OUTPUT_PATH, "r", encoding="utf-8") as f:
                for line in f:
                    if not line.strip():
                        continue
                    obj = json.loads(line)
                    k = obj.get("key")
                    if k:
                        processed_keys.add(k)
            print(f"🔄 Reanudando desde {len(processed_keys)} preguntas ya evaluadas (JSONL).")
        except Exception as e:
            print("⚠️ Error leyendo OUTPUT_PATH existente. Puede que el archivo esté corrupto. Ignorando y continuando.", e)

    max_workers = 5
    buffer = []
    save_every = 40  # <--- guardar cada 40

    try:
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = {}
            for k, v in data.items():
                if k in processed_keys:
                    continue
                futures[executor.submit(process_question, k, v, processed_keys)] = k

            with open(OUTPUT_PATH, "a", encoding="utf-8") as out_f:
                for fut in as_completed(futures):
                    res = fut.result()
                    if res is None:
                        continue
                    key, entry = res
                    line_obj = {"key": key, "entry": entry}
                    buffer.append(line_obj)
                    processed_keys.add(key)
                    print(f"✅ Pregunta {key} evaluada y añadida al buffer.")

                    # Guardar cada 40 entradas
                    if len(buffer) >= save_every:
                        for item in buffer:
                            out_f.write(json.dumps(item, ensure_ascii=False) + "\n")
                        out_f.flush()
                        buffer.clear()
                        print(f"💾 Guardadas 40 entradas al JSONL.")

        # Volcar cualquier entrada restante
        if buffer:
            with open(OUTPUT_PATH, "a", encoding="utf-8") as out_f:
                for item in buffer:
                    out_f.write(json.dumps(item, ensure_ascii=False) + "\n")
                out_f.flush()
            print(f"💾 Guardadas las últimas {len(buffer)} entradas al JSONL.")

    except KeyboardInterrupt:
        print("✋ Interrumpido por usuario. Lo procesado ya está en JSONL.")
    except Exception as e:
        print("❌ Error inesperado en ejecución:", e)

    print("✅ Proceso finalizado.")

if __name__ == "__main__":
    main()