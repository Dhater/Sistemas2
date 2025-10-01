import os
import json
import psycopg2
from psycopg2.extras import execute_values
from dotenv import load_dotenv
import shutil
import platform

# --- Cargar .env ---
load_dotenv()

DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT", 5432)
DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")

if platform.system() == "Windows" and DB_HOST == "database":
    DB_HOST = "localhost"

# --- Rutas ---
BASE_DIR = os.path.dirname(__file__)
JSON_ORIGINAL = os.path.join(BASE_DIR, "../data/grok_answers.json")
JSON_EVALUATED = os.path.join(BASE_DIR, "../data/grok_answers_evaluated.jsonl")

# Contar cantidad de datos en el JSON evaluado
if os.path.exists(JSON_EVALUATED):
    with open(JSON_EVALUATED, "r", encoding="utf-8") as f:
        if JSON_EVALUATED.endswith(".jsonl"):
            total_lines = sum(1 for _ in f)
        else:
            data = json.load(f)
            total_lines = len(data)
else:
    total_lines = 0

# --- Eliminar backups anteriores ---
for file in os.listdir(BASE_DIR):
    if file.startswith("grok_answers_backup_") and file.endswith(".jsonl"):
        os.remove(os.path.join(BASE_DIR, file))
        print(f"🗑 Eliminado backup anterior: {file}")

# Crear backup usando la cantidad de datos
LOCAL_BACKUP = os.path.join(BASE_DIR, f"grok_answers_backup_{total_lines}_entries.jsonl")
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

def generate_column_counts_json():
    """Genera un JSON con la cantidad de registros no nulos por columna"""
    with conn.cursor() as cur:
        cur.execute("""
            SELECT 
                COUNT(*) as total,
                COUNT(question_text) as question_text,
                COUNT(human_answer) as human_answer,
                COUNT(llm_answer) as llm_answer,
                COUNT(similarity_score) as similarity_score,
                COUNT(quality_score) as quality_score,
                COUNT(completeness_score) as completeness_score,
                COUNT(overall_score) as overall_score,
                COUNT(created_at) as created_at,
                COUNT(evaluated_at) as evaluated_at
            FROM questions
        """)
        counts = cur.fetchone()

    result = {
        "total": counts[0],
        "question_text": counts[1],
        "human_answer": counts[2],
        "llm_answer": counts[3],
        "similarity_score": counts[4],
        "quality_score": counts[5],
        "completeness_score": counts[6],
        "overall_score": counts[7],
        "created_at": counts[8],
        "evaluated_at": counts[9]
    }

    json_path = os.path.join(BASE_DIR, "column_counts.json")
    with open(json_path, "w", encoding="utf-8") as f:
        json.dump(result, f, ensure_ascii=False, indent=2)
    print(f"📄 Cantidad de registros por columna guardada en: {json_path}")

def upsert_json_file(file_path):
    batch = []
    batch_size = 1000
    total_count = 0

    if not os.path.exists(file_path):
        print(f"❌ No se encontró {file_path}, saltando.")
        return 0

    if file_path.endswith(".jsonl"):
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
    count_original = upsert_json_file(JSON_ORIGINAL)
    print(f"📥 Se introdujeron {count_original} preguntas desde {os.path.basename(JSON_ORIGINAL)}")

    count_evaluated = upsert_json_file(JSON_EVALUATED)
    print(f"📥 Se introdujeron {count_evaluated} preguntas desde {os.path.basename(JSON_EVALUATED)}")

    # Generar JSON con cantidad de datos por columna
    generate_column_counts_json()


if __name__ == "__main__":
    main()
