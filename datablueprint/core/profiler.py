import logging
import polars as pl
from pathlib import Path
from typing import Any, Dict

logger = logging.getLogger("DataBlueprint.Core.Profiler")

def _extract_metadata(df: pl.DataFrame, file_path: Path) -> Dict[str, Any]:
    """
    Funcion interna que toma un DataFrame de Polars ya cargado en memoria 
    y extrae toda la metrica de perfiles.
    """
    rows = df.height
    cols = df.width
    
    # 1. Metadatos del Sistema
    system_metadata = {
        "file_name": file_path.name,
        "file_path": f"{file_path.parent.name}/{file_path.name}",
        "size_bytes": file_path.stat().st_size,
        "extension": file_path.suffix.lower()
    }
    
    # 2. Metadatos Estructurales
    structural_metadata = {
        "total_rows": rows,
        "total_columns": cols,
        "columns_list": df.columns
    }
    
    # 3. Metadatos de Esquema y Calidad
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
            # TODO memory_usage
        }
        
        if series.dtype in pl.NUMERIC_DTYPES:
            col_info["min"] = series.min()
            col_info["max"] = series.max()
            col_info["mean"] = round(series.mean(), 4) if series.mean() is not None else None

        # TODO dispersion metrics (std_dev, variance)
        # TODO percentils (p25, p50, p75, p95)
        # TODO zeros_perc, negatives_perc
        # TODO compressed histogram ([0-10: 45%, 10-20: 30%, 20+: 25%])


        # TODO TopN frequencies (Top 3: ["Madrid" (40%), "Barcelona" (30%), "Valencia" (10%)])
        # TODO length metrics (min_length, max_length, mean_length)
        # TODO blanks vs voids
            
        schema_metadata[col_name] = col_info
        
    # 4. Muestra de Datos
    sample_data = df.head(3).to_dicts()
    
    return {
        "system": system_metadata,
        "structure": structural_metadata,
        "schema": schema_metadata,
        "sample": sample_data
    }

def process_csv(file_path: Path) -> Dict[str, Any]:
    logger.info(f"Leyendo CSV: {file_path.name}")
    df = pl.read_csv(file_path, infer_schema_length=10000, ignore_errors=True)
    return _extract_metadata(df, file_path)

def process_parquet(file_path: Path) -> Dict[str, Any]:
    """Lee archivos Parquet. Es el formato mas nativo y rapido para Polars."""
    logger.info(f"Leyendo Parquet: {file_path.name}")
    df = pl.read_parquet(file_path)
    return _extract_metadata(df, file_path)

def process_json(file_path: Path) -> Dict[str, Any]:
    """Maneja tanto JSON estandar como JSONL (Newline Delimited JSON)."""
    logger.info(f"Leyendo JSON: {file_path.name}")
    ext = file_path.suffix.lower()
    
    if ext == '.jsonl':
        df = pl.read_ndjson(file_path)
    else:
        df = pl.read_json(file_path)
        
    return _extract_metadata(df, file_path)