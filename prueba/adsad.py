#!/usr/bin/env python3
"""
count_answers.py

Cuenta:
 - total de preguntas en el archivo JSON
 - cuántas tienen llm_answer (no nulo y no vacío)

Uso:
  python count_answers.py --file path/to/grok_answers.json
"""

import argparse
import json
import os
from typing import Tuple

def load_json(path: str):
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)

def count_from_mapping(data: dict) -> Tuple[int,int]:
    """
    data expected like { "1": { ... }, "2": { ... }, ... }
    """
    total = 0
    answered = 0
    for key, item in data.items():
        # only consider entries that look like question objects
        if not isinstance(item, dict):
            continue
        total += 1
        llm = item.get("llm_answer", None)
        if llm is not None and str(llm).strip() != "":
            answered += 1
    return total, answered

def count_from_list(data: list) -> Tuple[int,int]:
    """
    data expected like [ { "id":..., "llm_answer": ... }, ... ]
    """
    total = 0
    answered = 0
    for item in data:
        if not isinstance(item, dict):
            continue
        total += 1
        llm = item.get("llm_answer", None)
        if llm is not None and str(llm).strip() != "":
            answered += 1
    return total, answered

def main():
    parser = argparse.ArgumentParser(description="Cuenta preguntas totales y respondidas en un JSON de backup.")
    parser.add_argument("--file", "-f", required=True, help="Ruta al JSON (ej: grok_answers.json)")
    args = parser.parse_args()

    path = args.file
    if not os.path.exists(path):
        print(f"Error: archivo no encontrado: {path}")
        return

    data = load_json(path)

    if isinstance(data, dict):
        total, answered = count_from_mapping(data)
    elif isinstance(data, list):
        total, answered = count_from_list(data)
    else:
        print("Formato JSON no reconocido. Debe ser un dict o una lista.")
        return

    print(f"Archivo: {path}")
    print(f"Total preguntas : {total}")
    print(f"Respondidas (llm_answer) : {answered}")
    if total > 0:
        pct = answered / total * 100
        print(f"Porcentaje respondidas: {pct:.2f}%")

if __name__ == "__main__":
    main()
