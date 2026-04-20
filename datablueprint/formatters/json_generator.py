import json
import logging
from typing import Dict, Any, List
from datetime import datetime, date
from decimal import Decimal

logger = logging.getLogger("DataBlueprint.Formatter.JSON")

class CustomJSONEncoder(json.JSONEncoder):
    """
    Codificador personalizado para JSON.
    Permite serializar tipos de datos nativos como datetime, date o Decimal
    que provienen de la extraccion de Polars en archivos Parquet.
    """
    def default(self, obj: Any) -> Any:
        # 1. Manejo de Fechas
        if isinstance(obj, (datetime, date)):
            return obj.isoformat()
        
        # 2. Manejo de Numeros Decimales de alta precision
        if isinstance(obj, Decimal):
            return float(obj)
            
        # Llamada por defecto si no es ninguno de los anteriores
        return super().default(obj)

def generate_aggregated_json(metadata_list: List[Dict[str, Any]], folder_name: str) -> str:
    """
    Genera un documento JSON consolidado que contiene los metadatos globales 
    del directorio y el detalle de cada archivo procesado.
    """
    logger.info("Generando reporte consolidado en formato JSON...")
    
    catalog = {
        "catalog_info": {
            "target_directory": folder_name,
            "total_files_processed": len(metadata_list),
            "generated_at": datetime.now().isoformat()
        },
        "files": metadata_list
    }
    
    # Inyectamos nuestro CustomJSONEncoder en el parametro cls
    return json.dumps(
        catalog, 
        indent=4, 
        ensure_ascii=False, 
        cls=CustomJSONEncoder
    )