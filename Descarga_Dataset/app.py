import os
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

def descargar_dataset(max_rows=30000):
    """Descarga el dataset de Kaggle y lo guarda en volumen y carpeta local, limitado a max_rows filas"""
    os.makedirs(DATASET_DIR, exist_ok=True)
    
    api = KaggleApi()
    api.authenticate()  # requiere KAGGLE_USERNAME y KAGGLE_KEY en el entorno

    print("Descargando dataset de Kaggle...")
    api.dataset_download_files(
        'jarupula/yahoo-answers-dataset',
        path=DATASET_DIR,
        unzip=True
    )
    
    # Buscar CSV descargado
    archivos_csv = [f for f in os.listdir(DATASET_DIR) if f.endswith('.csv')]
    if not archivos_csv:
        raise FileNotFoundError(f"No se encontró ningún CSV en {DATASET_DIR}")
    
    csv_descargado = os.path.join(DATASET_DIR, archivos_csv[0])

    # Cargar CSV en pandas (sin cabecera)
    df = pd.read_csv(
        csv_descargado,
        header=None,
        quotechar='"',
        doublequote=True,
        keep_default_na=False,
        dtype=str
    )

    # Normalizar columnas
    if df.shape[1] >= 4:
        df.columns = ['id', 'question_title', 'question_body', 'human_answer'] + [f'col{i}' for i in range(4, df.shape[1])]
        df['question_text'] = df['question_title'].fillna('') + ' ' + df['question_body'].fillna('')
    else:
        df.columns = ['question_text', 'human_answer'] + [f'col{i}' for i in range(2, df.shape[1])]
        df['question_text'] = df['question_text'].fillna('')

    # Evitar valores nulos
    df['human_answer'] = df['human_answer'].fillna('')
    df['llm_answer'] = ''

    # Limitar a las primeras max_rows filas
    df = df.head(max_rows)

    # Guardar CSV en volumen compartido y carpeta local
    df[['question_text', 'human_answer', 'llm_answer']].to_csv(
        CSV_PATH_VOLUME, index=False, quoting=1, line_terminator='\n'
    )
    df[['question_text', 'human_answer', 'llm_answer']].to_csv(
        LOCAL_CSV_PATH, index=False, quoting=1, line_terminator='\n'
    )

    print("Dataset guardado en volumen:", CSV_PATH_VOLUME)
    print("Dataset también guardado en proyecto:", LOCAL_CSV_PATH)
    print(f"Tamaño: {len(df)} registros")
    print("Columnas:", df.columns.tolist())

    return df

def insertar_en_postgres(df):
    """Inserta las preguntas en PostgreSQL usando COPY desde un StringIO seguro"""
    conn = psycopg2.connect(
        host=DB_HOST,
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASS,
        port=DB_PORT
    )
    conn.autocommit = True
    cur = conn.cursor()

    # Crear un buffer CSV para COPY
    buffer = StringIO()
    df[['question_text', 'human_answer', 'llm_answer']].to_csv(
        buffer, index=False, header=False, sep='\t', quoting=3
    )
    buffer.seek(0)

    print("Insertando datos en PostgreSQL...")
    cur.copy_from(buffer, 'questions', sep='\t', columns=('question_text', 'human_answer', 'llm_answer'))
    print(f"Se insertaron {len(df)} filas en la tabla questions")

    cur.close()
    conn.close()

if __name__ == "__main__":
    df = descargar_dataset(max_rows=30000)
    insertar_en_postgres(df)
