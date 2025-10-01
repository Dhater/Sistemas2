# upsert_jsonl_pg.py
import os
import json
import psycopg2
from psycopg2.extras import execute_values
from dotenv import load_dotenv
import shutil
import platform
from datetime import datetime

# --- Cargar .env ---
load_dotenv()

DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT", 5432)
DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")

# Sobrescribir DB_HOST para Windows si queremos probar fuera de Docker
if platform.system() == "Windows" and DB_HOST == "database":
    DB_HOST = "localhost"

# --- Rutas ---
BASE_DIR = os.path.dirname(__file__)
JSON_ORIGINAL = os.path.join(BASE_DIR, "../data/grok_answers.json")
JSON_EVALUATED = os.path.join(BASE_DIR, "../data/grok_answers_evaluated.jsonl")
LOCAL_BACKUP = os.path.join(BASE_DIR, f"grok_answers_backup_{datetime.now().strftime('%Y%m%d_%H%M%S')}.jsonl")
DATABASE_DUMP = os.path.join(BASE_DIR, "database_copy.json")

# --- Crear copia local del JSON evaluated ---
if os.path.exists(JSON_EVALUATED):
    shutil.copy2(JSON_EVALUATED, LOCAL_BACKUP)
    print(f"📄 Copia local creada en: {LOCAL_BACKUP}")
else:
    print(f"⚠️ No se encontró {JSON_EVALUATED}, se continuará sin backup de evaluated.")

# --- Conexión a PostgreSQL ---
try:
    conn = psycopg2.connect(
        host=DB_HOST,
        port=DB_PORT,
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD
    )
    conn.autocommit = True
    print("✅ Conexión a PostgreSQL exitosa.")
except Exception as e:
    print("❌ Error de conexión:", e)
    exit(1)

def upsert_questions(data):
    """Upsert de lista de dicts tipo {"key": ..., "entry": {...}}"""
    records = []
    for item in data:
        entry = item["entry"]
        records.append((
            int(item["key"]),
            entry.get("question_text"),
            entry.get("human_answer"),
            entry.get("llm_answer"),
            entry.get("similarity_score"),
            entry.get("quality_score"),
            entry.get("completeness_score"),
            entry.get("overall_score"),
            entry.get("created_at"),
            entry.get("evaluated_at")
        ))

    query = """
    INSERT INTO questions (
        id, question_text, human_answer, llm_answer,
        similarity_score, quality_score, completeness_score, overall_score,
        created_at, evaluated_at
    )
    VALUES %s
    ON CONFLICT (id) DO UPDATE SET
        llm_answer = EXCLUDED.llm_answer,
        similarity_score = EXCLUDED.similarity_score,
        quality_score = EXCLUDED.quality_score,
        completeness_score = EXCLUDED.completeness_score,
        overall_score = EXCLUDED.overall_score,
        evaluated_at = EXCLUDED.evaluated_at,
        question_text = CASE WHEN questions.question_text IS NULL OR questions.question_text = '' THEN EXCLUDED.question_text ELSE questions.question_text END,
        human_answer = CASE WHEN questions.human_answer IS NULL OR questions.human_answer = '' THEN EXCLUDED.human_answer ELSE questions.human_answer END
    ;
    """
    with conn.cursor() as cur:
        execute_values(cur, query, records)
    print(f"✅ Insert/Update de {len(records)} preguntas completado.")

def dump_database_to_json():
    """Crea un JSON local con todos los registros actuales de la base de datos"""
    with conn.cursor() as cur:
        cur.execute("""
            SELECT id, question_text, human_answer, llm_answer,
                   similarity_score, quality_score, completeness_score,
                   overall_score, created_at, evaluated_at
            FROM questions
            ORDER BY id
        """)
        rows = cur.fetchall()
        result = []
        for r in rows:
            result.append({
                "id": r[0],
                "question_text": r[1],
                "human_answer": r[2],
                "llm_answer": r[3],
                "similarity_score": r[4],
                "quality_score": r[5],
                "completeness_score": r[6],
                "overall_score": r[7],
                "created_at": r[8].isoformat() if r[8] else None,
                "evaluated_at": r[9].isoformat() if r[9] else None
            })
    with open(DATABASE_DUMP, "w", encoding="utf-8") as f:
        json.dump(result, f, ensure_ascii=False, indent=2)
    print(f"📄 Dump de la base de datos guardado en: {DATABASE_DUMP}")

def upsert_json_file(file_path):
    """Sube el contenido de un JSON o JSONL a la base de datos
    Devuelve la cantidad total de registros insertados/upserted
    """
    batch = []
    batch_size = 1000
    total_count = 0

    if not os.path.exists(file_path):
        print(f"❌ No se encontró {file_path}, saltando.")
        return 0

    if file_path.endswith(".jsonl"):
        # JSONL: cada línea es un objeto tipo {"key":..., "entry": {...}}
        with open(file_path, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                obj = json.loads(line)
                batch.append(obj)
                total_count += 1
                if len(batch) >= batch_size:
                    upsert_questions(batch)
                    batch.clear()
    else:
        # JSON completo tipo {"3": {...}, "7": {...}, ...}
        with open(file_path, "r", encoding="utf-8") as f:
            data = json.load(f)
            for key, entry in data.items():
                batch.append({"key": key, "entry": entry})
                total_count += 1
                if len(batch) >= batch_size:
                    upsert_questions(batch)
                    batch.clear()

    if batch:
        upsert_questions(batch)

    return total_count



def main():
    # 1️⃣ Primero subir grok_answers.json
    count_original = upsert_json_file(JSON_ORIGINAL)
    print(f"📥 Se introdujeron {count_original} preguntas desde {os.path.basename(JSON_ORIGINAL)}")

    # 2️⃣ Luego subir grok_answers_evaluated.jsonl
    count_evaluated = upsert_json_file(JSON_EVALUATED)
    print(f"📥 Se introdujeron {count_evaluated} preguntas desde {os.path.basename(JSON_EVALUATED)}")

    # 3️⃣ Dump completo de la base
    dump_database_to_json()


if __name__ == "__main__":
    main()
