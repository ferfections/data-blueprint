import logging
from pathlib import Path
from typing import Dict, Any

from datablueprint.core.profiler import process_csv, process_parquet, process_json
from datablueprint.formatters.ddl_generator import generate_sql_ddl

logger = logging.getLogger("DataBlueprint.API.Service")

def generate_blueprint_for_file(file_path: Path) -> Dict[str, Any]:
    """
    Toma un archivo del workspace temporal y decide qué función de Polars usar
    basándose en su extensión.
    """
    ext = file_path.suffix.lower()
    
    try:
        if ext == '.csv':
            return process_csv(file_path)
        elif ext == '.parquet':
            return process_parquet(file_path)
        elif ext in ['.json', '.jsonl']:
            return process_json(file_path)
        else:
            raise ValueError(f"Formato de archivo no soportado: {ext}")
    except Exception as e:
        logger.error(f"Error procesando {file_path.name}: {str(e)}")
        raise e