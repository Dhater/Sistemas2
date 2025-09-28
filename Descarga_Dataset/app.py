# app.py
import os
import pandas as pd
from kagglehub import KaggleDatasetAdapter
import kagglehub
import psycopg2
from io import StringIO

DB_HOST = os.environ.get("DB_HOST", "database")
DB_NAME = os.environ.get("DB_NAME")
DB_USER = os.environ.get("DB_USER")
DB_PASS = os.environ.get("DB_PASSWORD")
DB_PORT = os.environ.get("DB_PORT", "5432")

CSV_PATH = '/data/yahoo_answers.csv'

def descargar_dataset():
    df = kagglehub.load_dataset(
        KaggleDatasetAdapter.PANDAS,
        "jarupula/yahoo-answers-dataset",
        ""
    )
    df.to_csv(CSV_PATH, index=False)
    print("Dataset descargado y guardado:", CSV_PATH)
    print(f"Tamaño: {len(df)} registros")
    print("Columnas:", df.columns.tolist())
    return df

def csv_to_postgres_copy(csv_path, table_name='questions', chunk_size=50000):
    conn = psycopg2.connect(host=DB_HOST, dbname=DB_NAME, user=DB_USER, password=DB_PASS, port=DB_PORT)
    conn.autocommit = True
    cur = conn.cursor()

    # Ajusta estos nombres según las columnas reales del CSV
    # Ejemplo común: 'question' y 'best_answer' o 'answer'
    # Mapearemos a los campos (question_text, human_answer, llm_answer)
    for chunk in pd.read_csv(csv_path, chunksize=chunk_size):
        # Normalizar columnas: elegir las columnas existentes
        if 'question' in chunk.columns:
            chunk['question_text'] = chunk['question']
        elif 'question_title' in chunk.columns:
            chunk['question_text'] = chunk['question_title']
        elif 'question_body' in chunk.columns:
            chunk['question_text'] = chunk['question_body']
        else:
            # si no hay columna reconocida, crea a partir de la primera columna
            chunk['question_text'] = chunk.iloc[:, 0].astype(str)

        # intentar encontrar respuesta humana en columnas conocidas
        if 'best_answer' in chunk.columns:
            chunk['human_answer'] = chunk['best_answer'].fillna('')
        elif 'answer' in chunk.columns:
            chunk['human_answer'] = chunk['answer'].fillna('')
        else:
            # Si no hay respuesta, dejamos campo vacío para no violar NOT NULL
            chunk['human_answer'] = ''

        # llm_answer vacío al inicio
        chunk['llm_answer'] = ''

        # Seleccionamos sólo las columnas que vamos a insertar
        to_insert = chunk[['question_text', 'human_answer', 'llm_answer']].astype(str)

        # Usar COPY FROM con StringIO
        buffer = StringIO()
        to_insert.to_csv(buffer, index=False, header=False, sep='\t', quoting=3)  # quoting=3 -> csv.QUOTE_NONE
        buffer.seek(0)

        # COPY ... FROM STDIN con columnas especificadas
        cur.copy_from(buffer, table_name, sep='\t', null='', columns=('question_text','human_answer','llm_answer'))
        print(f"Insertado chunk de {len(to_insert)} filas")

    cur.close()
    conn.close()

if __name__ == "__main__":
    df = descargar_dataset()
    # Omitir descarga si ya existe CSV: si no quieres descargar otra vez, comenta la línea anterior.
    csv_to_postgres_copy(CSV_PATH)
