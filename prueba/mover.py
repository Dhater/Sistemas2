import os
import shutil

def copy_grok_to_localdata():
    # Carpeta actual del script
    base_dir = os.path.dirname(os.path.abspath(__file__))

    # Ruta del archivo de origen (relativa a prueba/)
    source_path = os.path.join(base_dir, "grok_answers.json")

    # Carpeta de destino relativa al proyecto
    target_dir = os.path.join(base_dir, "..", "local_data")
    os.makedirs(target_dir, exist_ok=True)

    # Ruta completa de destino
    target_path = os.path.join(target_dir, "grok_answers.json")

    if not os.path.exists(source_path):
        print(f"❌ No se encontró el archivo en {source_path}")
        return

    shutil.copy2(source_path, target_path)
    print(f"✅ Archivo copiado a {target_path}")

if __name__ == "__main__":
    copy_grok_to_localdata()
