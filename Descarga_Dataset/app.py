import os
import csv
import pandas as pd
from kaggle.api.kaggle_api_extended import KaggleApi
import psycopg2
from io import StringIO

# Volumen compartido dentro del contenedor
VOLUME_DIR = '/data'
CSV_PATH_VOLUME = os.path.join(VOLUME_DIR, 'yahoo_answers.csv')
DATASET_DIR = os.path.join(VOLUME_DIR, 'yahoo_dataset')

# Carpeta local del proyecto
LOCAL_CSV_PATH = os.path.join(os.getcwd(), 'yahoo_answers.csv')

# Variables de entorno para PostgreSQL
DB_HOST = os.environ.get("DB_HOST", "database")
DB_NAME = os.environ.get("DB_NAME")
DB_USER = os.environ.get("DB_USER")
DB_PASS = os.environ.get("DB_PASSWORD")
DB_PORT = os.environ.get("DB_PORT", "5432")

CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS questions (
    id SERIAL PRIMARY KEY,
    question_text TEXT NOT NULL,
    human_answer TEXT NOT NULL,
    llm_answer TEXT,
    similarity_score FLOAT,
    quality_score FLOAT,
    completeness_score FLOAT,
    overall_score FLOAT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    evaluated_at TIMESTAMP
);
"""

def descargar_dataset(max_rows=30000):
    # Si ya existe el CSV en el volumen compartido, lo usamos directamente
    if os.path.exists(CSV_PATH_VOLUME):
        print(f"CSV ya existe en {CSV_PATH_VOLUME}, cargando directamente...")
        df = pd.read_csv(CSV_PATH_VOLUME, dtype=str, encoding='utf-8')
        df['human_answer'] = df['human_answer'].fillna('N/A')
        df['llm_answer'] = df['llm_answer'].fillna('')
        return df.head(max_rows)

    # Si no existe, lo descargamos desde Kaggle
    os.makedirs(DATASET_DIR, exist_ok=True)
    api = KaggleApi()
    api.authenticate()  # requiere KAGGLE_USERNAME y KAGGLE_KEY en el entorno

    print("Descargando dataset de Kaggle...")
    api.dataset_download_files(
        'jarupula/yahoo-answers-dataset',
        path=DATASET_DIR,
        unzip=True
    )

    archivos_csv = [f for f in os.listdir(DATASET_DIR) if f.endswith('.csv')]
    if not archivos_csv:
        raise FileNotFoundError(f"No se encontró ningún CSV en {DATASET_DIR}")
    csv_descargado = os.path.join(DATASET_DIR, archivos_csv[0])

    try:
        df = pd.read_csv(
            csv_descargado,
            header=None,
            quotechar='"',
            doublequote=True,
            keep_default_na=False,
            dtype=str,
            encoding='utf-8',
            on_bad_lines='skip',
            low_memory=False,
            engine='c'
        )
    except ValueError as e:
        print("Warning: lectura con engine C falló:", e)
        df = pd.read_csv(
            csv_descargado,
            header=None,
            quotechar='"',
            doublequote=True,
            keep_default_na=False,
            dtype=str,
            encoding='utf-8',
            on_bad_lines='skip'
        )

    # Normalizar columnas
    if df.shape[1] >= 4:
        df.columns = ['id', 'question_title', 'question_body', 'human_answer'] + [f'col{i}' for i in range(4, df.shape[1])]
        df['question_text'] = df['question_title'].fillna('') + ' ' + df['question_body'].fillna('')
    else:
        df.columns = ['question_text', 'human_answer'] + [f'col{i}' for i in range(2, df.shape[1])]
        df['question_text'] = df['question_text'].fillna('')

    # Asegurar que no haya valores nulos en human_answer
    df['human_answer'] = df['human_answer'].fillna('N/A')
    df['llm_answer'] = ''

    df = df.head(max_rows)

    # Guardar CSV en volumen compartido y carpeta local
    df[['question_text', 'human_answer', 'llm_answer']].to_csv(
        CSV_PATH_VOLUME,
        index=False,
        quoting=csv.QUOTE_MINIMAL,
        lineterminator='\n',
        encoding='utf-8'
    )
    df[['question_text', 'human_answer', 'llm_answer']].to_csv(
        LOCAL_CSV_PATH,
        index=False,
        quoting=csv.QUOTE_MINIMAL,
        lineterminator='\n',
        encoding='utf-8'
    )

    print("Dataset guardado en volumen:", CSV_PATH_VOLUME)
    print("Dataset también guardado en proyecto:", LOCAL_CSV_PATH)
    print(f"Tamaño: {len(df)} registros")
    print("Columnas:", df.columns.tolist())

    return df

def insertar_en_postgres_via_copy(df, restart_identity=True):
    conn = psycopg2.connect(
        host=DB_HOST,
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASS,
        port=DB_PORT
    )
    conn.autocommit = True
    try:
        cur = conn.cursor()
        cur.execute(CREATE_TABLE_SQL)
        conn.commit()

        cur.execute("SELECT COUNT(*) FROM questions;")
        count = cur.fetchone()[0]

        if count > 0:
            if restart_identity:
                print(f"La tabla 'questions' tiene {count} registros. Vaciando con TRUNCATE ... RESTART IDENTITY")
                cur.execute("TRUNCATE TABLE questions RESTART IDENTITY;")
            else:
                print(f"La tabla 'questions' tiene {count} registros. Vaciando con TRUNCATE (sin reiniciar identity).")
                cur.execute("TRUNCATE TABLE questions;")
            conn.commit()

        buffer = StringIO()
        df[['question_text', 'human_answer', 'llm_answer']].to_csv(
            buffer,
            index=False,
            header=False,
            sep='\t',
            quoting=csv.QUOTE_MINIMAL,
            quotechar='"',
            lineterminator='\n',
            encoding='utf-8'
        )
        buffer.seek(0)

        sql = "COPY questions (question_text, human_answer, llm_answer) " \
              "FROM STDIN WITH (FORMAT csv, DELIMITER E'\\t', QUOTE '\"', NULL '');"
        print("Insertando datos en PostgreSQL con COPY ... FROM STDIN (copy_expert)...")
        cur.copy_expert(sql, buffer)
        conn.commit()
        print(f"Se insertaron {len(df)} filas en la tabla questions")

        cur.close()
    finally:
        conn.close()

if __name__ == "__main__":
    missing = [k for k,v in {
        "DB_NAME": DB_NAME,
        "DB_USER": DB_USER,
        "DB_PASSWORD": DB_PASS
    }.items() if not v]
    if missing:
        raise EnvironmentError(f"Faltan variables de entorno necesarias: {', '.join(missing)}")

    df = descargar_dataset(max_rows=30000)
    insertar_en_postgres_via_copy(df, restart_identity=True)
