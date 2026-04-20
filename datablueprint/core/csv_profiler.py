import logging
import polars as pl
from pathlib import Path
from typing import Any, Dict

logger = logging.getLogger("DataBlueprint.Core.CSV")

def process_csv_with_polars(file_path: Path) -> Dict[str, Any]:
    """
    Lee un archivo CSV usando Polars y extrae metadatos a nivel de sistema, 
    estructura, esquema y una muestra de los datos.
    """
    logger.info(f"Iniciando perfilado del archivo: {file_path.name}")
    
    # 1. Metadatos del Sistema (Libreria estandar)
    system_metadata = {
        "file_name": file_path.name,
        # Usamos file_path.parent.name para obtener solo la carpeta contenedora
        "file_path": f"{file_path.parent.name}/{file_path.name}",
        "size_bytes": file_path.stat().st_size,
        "extension": file_path.suffix.lower()
    }
    
    try:
        # 2. Lectura eficiente
        # infer_schema_length=10000 lee solo las primeras 10000 filas para deducir 
        # los tipos de datos en lugar de escanear un archivo gigante entero.
        # ignore_errors=True evita que una fila mal formateada detenga todo el proceso.
        df = pl.read_csv(
            file_path, 
            infer_schema_length=10000, 
            ignore_errors=True
        )
        
        rows = df.height
        cols = df.width
        
        # 3. Metadatos Estructurales
        structural_metadata = {
            "total_rows": rows,
            "total_columns": cols,
            "columns_list": df.columns
        }
        
        # 4. Metadatos de Esquema y Calidad (Por columna)
        schema_metadata = {}
        
        for col_name in df.columns:
            series = df[col_name]
            dtype_str = str(series.dtype)
            null_count = series.null_count()
            
            col_info = {
                "data_type": dtype_str,
                "null_count": null_count,
                "null_percentage": round((null_count / rows * 100), 2) if rows > 0 else 0.0,
                "unique_values": series.n_unique()
            }
            
            # Estadisticas adicionales solo si la columna es numerica
            if series.dtype in pl.NUMERIC_DTYPES:
                col_info["min"] = series.min()
                col_info["max"] = series.max()
                col_info["mean"] = round(series.mean(), 4) if series.mean() is not None else None
                
            schema_metadata[col_name] = col_info
            
        # 5. Muestra de Datos (Sample)
        # Extraemos las primeras 3 filas y las convertimos a diccionarios
        sample_data = df.head(3).to_dicts()
        
        logger.info(f"Perfilado completado con exito para: {file_path.name}")
        
        # Ensamblamos el diccionario final de salida
        return {
            "system": system_metadata,
            "structure": structural_metadata,
            "schema": schema_metadata,
            "sample": sample_data
        }
        
    except Exception as e:
        logger.error(f"Fallo critico procesando el CSV {file_path.name}: {str(e)}")
        raise