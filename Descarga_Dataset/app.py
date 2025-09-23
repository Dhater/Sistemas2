import pandas as pd
import os
from kagglehub import KaggleDatasetAdapter
import kagglehub

def descargar_dataset():
    # Descargar dataset usando kagglehub
    df = kagglehub.load_dataset(
        KaggleDatasetAdapter.PANDAS,
        "jarupula/yahoo-answers-dataset",
        ""
    )
    
    # Guardar en volumen compartido
    df.to_csv('/data/yahoo_answers.csv', index=False)
    print("Dataset descargado y guardado exitosamente")
    print(f"Tamaño del dataset: {len(df)} registros")
    print(f"Columnas: {df.columns.tolist()}")

if __name__ == "__main__":
    descargar_dataset()