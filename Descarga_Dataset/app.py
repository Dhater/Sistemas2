import os
import csv
import pandas as pd
from kaggle.api.kaggle_api_extended import KaggleApi

# Volumen compartido dentro del contenedor
VOLUME_DIR = '/data'
CSV_PATH_VOLUME = os.path.join(VOLUME_DIR, 'yahoo_answers.csv')
DATASET_DIR = os.path.join(VOLUME_DIR, 'yahoo_dataset')

# Carpeta local del proyecto
LOCAL_CSV_PATH = os.path.join(os.getcwd(), 'yahoo_answers.csv')

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

if __name__ == "__main__":
    df = descargar_dataset(max_rows=30000)
