# archivo: evaluate_glm_jsonl.py
import os
import json
import time
import re
from itertools import cycle
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock
from openai import OpenAI

# --- Configuración ---
API_KEYS = [k.strip() for k in os.getenv("OPENROUTER_API_KEY", "").split(",") if k.strip()]
if not API_KEYS:
    raise ValueError("No se encontraron API keys en OPENROUTER_API_KEY")
api_keys_cycle = cycle(API_KEYS)

DATA_PATH = "/data/grok_answers.json"            # input: JSON con las preguntas y respuestas
OUTPUT_PATH = "/data/grok_answers_evaluated.jsonl"  # salida: JSONL
MAX_ENTRIES = 10001  # límite total
SAVE_EVERY = 40      # guardar cada 40 resultados

lock = Lock()

# --- Helper para limpiar JSON ---
def safe_load_json_from_text(text: str):
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


# --- Llamada al modelo GLM-4.5 con rotación de keys ---
def call_glm(prompt, wait_on_fail=10):
    tried_keys = set()
    while len(tried_keys) < len(API_KEYS):
        key = next(api_keys_cycle)
        if key in tried_keys:
            continue
        tried_keys.add(key)

        try:
            client = OpenAI(base_url="https://openrouter.ai/api/v1", api_key=key)
            print(f"⏳ Llamando a GLM-4.5 con key {key[:8]}...")

            completion = client.chat.completions.create(
                model="z-ai/glm-4.5-air:free",
                messages=[{"role": "user", "content": prompt}],
                extra_headers={
                    "HTTP-Referer": "https://tu-sistema.local",
                    "X-Title": "Yahoo QA Evaluator"
                },
                timeout=60
            )

            content = completion.choices[0].message.content
            if not content:
                raise ValueError("Respuesta vacía del modelo.")
            print(f"✅ Respuesta recibida con key {key[:8]}")
            return content.strip()

        except Exception as e:
            print(f"❌ Error con key {key[:8]}: {e}. Rotando...")
            time.sleep(wait_on_fail)

    raise RuntimeError("Todas las API keys fallaron.")


# --- Evaluación de una respuesta ---
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
Devuelve SOLO JSON (sin texto adicional).
"""
    try:
        raw = call_glm(prompt)
        parsed = safe_load_json_from_text(raw)
        if parsed is None:
            print("⚠️ No se pudo extraer JSON. Texto devuelto:", raw[:200])
            return {"similarity_score": 0.0, "quality_score": 0.0, "completeness_score": 0.0}
        def to_float(v):
            try:
                return float(v)
            except:
                return 0.0
        return {
            "similarity_score": to_float(parsed.get("similarity_score", 0.0)),
            "quality_score": to_float(parsed.get("quality_score", 0.0)),
            "completeness_score": to_float(parsed.get("completeness_score", 0.0))
        }
    except Exception as e:
        print(f"❌ Error en evaluate_response: {e}")
        return {"similarity_score": 0.0, "quality_score": 0.0, "completeness_score": 0.0}


def calculate_overall(sim, qual, comp):
    return round(sim * 0.5 + qual * 0.3 + comp * 0.2, 6)


# --- Procesar una sola pregunta ---
def process_question(key, entry, processed_keys):
    if key in processed_keys:
        return None

    print(f"🧠 Evaluando pregunta {key}...")
    scores = evaluate_response(entry.get("human_answer", ""), entry.get("llm_answer", ""))
    overall = calculate_overall(scores["similarity_score"], scores["quality_score"], scores["completeness_score"])

    entry["similarity_score"] = scores["similarity_score"]
    entry["quality_score"] = scores["quality_score"]
    entry["completeness_score"] = scores["completeness_score"]
    entry["overall_score"] = overall
    entry["evaluated_at"] = datetime.utcnow().isoformat()

    return (key, entry)


# --- Main ---
def main():
    if not os.path.exists(DATA_PATH):
        print(f"❌ No se encontró el archivo de entrada: {DATA_PATH}")
        return

    with open(DATA_PATH, "r", encoding="utf-8") as f:
        data = json.load(f)

    # Leer progreso previo
    processed_keys = set()
    current_count = 0
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
                        current_count += 1
            print(f"🔄 Reanudando desde {len(processed_keys)} evaluaciones previas.")
        except Exception as e:
            print("⚠️ Error leyendo el JSONL previo, continuando desde cero:", e)

    if current_count >= MAX_ENTRIES:
        print(f"✅ Se alcanzó el límite máximo de {MAX_ENTRIES} evaluaciones.")
        return

    max_workers = 5
    buffer = []
    save_every = SAVE_EVERY

    try:
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = {}
            for k, v in data.items():
                if k in processed_keys:
                    continue
                if current_count >= MAX_ENTRIES:
                    print(f"⚠️ Se alcanzó el límite de {MAX_ENTRIES}. Deteniendo proceso.")
                    break
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
                    current_count += 1

                    print(f"✅ Pregunta {key} evaluada. Total={current_count}")

                    # Guardar cada SAVE_EVERY
                    if len(buffer) >= save_every:
                        for item in buffer:
                            out_f.write(json.dumps(item, ensure_ascii=False) + "\n")
                        out_f.flush()
                        buffer.clear()
                        print(f"💾 Guardadas {save_every} entradas al JSONL.")

                    if current_count >= MAX_ENTRIES:
                        print(f"⚠️ Se alcanzó el límite de {MAX_ENTRIES}. Finalizando ejecución.")
                        break

        # Guardar las últimas si hay
        if buffer:
            with open(OUTPUT_PATH, "a", encoding="utf-8") as out_f:
                for item in buffer:
                    out_f.write(json.dumps(item, ensure_ascii=False) + "\n")
                out_f.flush()
            print(f"💾 Guardadas las últimas {len(buffer)} entradas al JSONL.")

    except KeyboardInterrupt:
        print("✋ Proceso interrumpido manualmente.")
    except Exception as e:
        print(f"❌ Error inesperado: {e}")

    print("✅ Proceso completado.")


if __name__ == "__main__":
    main()
