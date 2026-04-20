import argparse
import sys
import logging
from pathlib import Path

# Configuracion del Logger (Nivel INFO por defecto)
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger("DataBlueprint")

# Extensiones soportadas
SUPPORTED_EXTENSIONS = {'.csv', '.parquet', '.json', '.jsonl'}
def setup_parser() -> argparse.ArgumentParser:
    """Configura y devuelve el parser de argumentos de la linea de comandos."""
    parser = argparse.ArgumentParser(
        description="DataBlueprint: Extrae metadatos de archivos para contexto de IA."
    )
    parser.add_argument(
        "input_path", 
        type=str, 
        help="Ruta al archivo o carpeta a analizar."
    )
    return parser

def get_files_to_process(target_path: Path) -> list[Path]:
    """Valida la ruta y extrae la lista de archivos soportados."""
    if not target_path.exists():
        logger.error(f"La ruta especificada no existe: {target_path}")
        sys.exit(1)

    files_to_process = []

    if target_path.is_file():
        if target_path.suffix.lower() in SUPPORTED_EXTENSIONS:
            files_to_process.append(target_path)
        else:
            logger.warning(f"Formato no soportado para el archivo: {target_path.name}")
            
    elif target_path.is_dir():
        logger.info(f"Escaneando directorio: {target_path}")
        for file_path in target_path.iterdir():
            if file_path.is_file() and file_path.suffix.lower() in SUPPORTED_EXTENSIONS:
                files_to_process.append(file_path)
                
    return files_to_process

