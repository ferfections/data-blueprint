import json
import logging
from typing import Dict, Any, List
from datetime import datetime

logger = logging.getLogger("DataBlueprint.Formatter.JSON")

def generate_aggregated_json(metadata_list: List[Dict[str, Any]], folder_name: str) -> str:
    """
    Genera un documento JSON consolidado que contiene los metadatos globales 
    del directorio y el detalle de cada archivo procesado.
    """
    logger.info("Generando reporte consolidado en formato JSON...")
    
    # Creamos la estructura raiz del catalogo
    catalog = {
        "catalog_info": {
            "target_directory": folder_name,
            "total_files_processed": len(metadata_list),
            "generated_at": datetime.now().isoformat()
        },
        "files": metadata_list
    }
    
    # json.dumps convierte el diccionario en un string JSON.
    # indent=4 lo hace legible para humanos (pretty print).
    # ensure_ascii=False respeta acentos y caracteres especiales.
    return json.dumps(catalog, indent=4, ensure_ascii=False)