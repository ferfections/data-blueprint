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
            valid_series = series.drop_nulls()
            total_valid = valid_series.len()

            if total_valid > 0:
                # Métricas Básicas
                col_info["min"] = round(valid_series.min(), 4)
                col_info["max"] = round(valid_series.max(), 4)
                col_info["mean"] = round(valid_series.mean(), 4)

                # Métricas de Dispersión
                col_info["std_dev"] = round(valid_series.std(), 4) if total_valid > 1 else 0.0
                col_info["variance"] = round(valid_series.var(), 4) if total_valid > 1 else 0.0

                # Percentiles (Cuantiles)
                col_info["p25"] = round(valid_series.quantile(0.25), 4)
                col_info["p50"] = round(valid_series.median(), 4)
                col_info["p75"] = round(valid_series.quantile(0.75), 4)
                col_info["p95"] = round(valid_series.quantile(0.95), 4)

                # Análisis de Signo (Porcentaje de Ceros y Negativos)
                zeros_count = (valid_series == 0).sum()
                negatives_count = (valid_series < 0).sum()
                
                col_info["zeros_perc"] = round((zeros_count / total_valid) * 100, 2)
                col_info["negatives_perc"] = round((negatives_count / total_valid) * 100, 2)

                # Histograma Comprimido (Optimización de Tokens)
                try:
                    # Creamos 5 "cubos" (bins) de forma nativa con Polars
                    hist_df = valid_series.hist(bin_count=5)
                    
                    count_col = [c for c in hist_df.columns if "count" in c.lower()][0]
                    bp_col = "break_point" # Límite superior del bin
                    
                    hist_parts = []
                    prev_bp = col_info["min"]
                    
                    for row in hist_df.iter_rows(named=True):
                        current_bp = row[bp_col]
                        count = row[count_col]
                        perc = round((count / total_valid) * 100, 1)
                        
                        # Solo añadimos el bin si tiene algún dato (para ahorrar tokens)
                        if perc > 0:
                            hist_parts.append(f"[{round(prev_bp, 2)}-{round(current_bp, 2)}: {perc}%]")
                        
                        prev_bp = current_bp
                        
                    col_info["histogram"] = ", ".join(hist_parts)
                except Exception as e:
                    # Fallback de seguridad en caso de que la columna tenga todos los valores iguales
                    col_info["histogram"] = "N/A"


        if series.dtype in [pl.String, pl.Categorical]:
            valid_series = series.drop_nulls()
            total_valid = valid_series.len()

            if total_valid > 0:
                # 1. Top N Frecuencias (Identificación de Enums o baja cardinalidad)
                try:
                    # value_counts() devuelve un DataFrame con el valor y su conteo
                    top_counts = valid_series.value_counts(sort=True).head(3)
                    
                    # Polars puede nombrar la columna de conteo como 'count' o 'counts' según la versión
                    count_col = "count" if "count" in top_counts.columns else "counts"
                    val_col = top_counts.columns[0] # El nombre real de la columna
                    
                    top_parts = []
                    for row in top_counts.iter_rows(named=True):
                        val = row[val_col]
                        cnt = row[count_col]
                        perc = round((cnt / total_valid) * 100, 1)
                        
                        # Formateo amigable para el LLM: "Madrid" (40.5%)
                        top_parts.append(f'"{val}" ({perc}%)')
                    
                    col_info["top_3_values"] = f"[{', '.join(top_parts)}]"
                    
                except Exception as e:
                    col_info["top_3_values"] = "N/A"

                # Para operaciones a nivel de caracteres, nos aseguramos de que sea String
                if series.dtype == pl.String:
                    # 2. Métricas de longitud
                    # Usamos len_chars() y no len_bytes() para que cuente bien los acentos (á) y emojis
                    lengths = valid_series.str.len_chars()
                    
                    col_info["min_length"] = lengths.min()
                    col_info["max_length"] = lengths.max()
                    col_info["mean_length"] = round(lengths.mean(), 2) if lengths.mean() is not None else 0

                    # a) Cadenas totalmente vacías
                    empty_count = (valid_series == "").sum()
                    
                    # b) Cadenas que solo contienen espacios (strip_chars las deja vacías)
                    # Excluimos las que ya son empty para no contarlas dos veces
                    void_count = ((valid_series != "") & (valid_series.str.strip_chars() == "")).sum()

                    col_info["empty_strings_perc"] = round((empty_count / total_valid) * 100, 2)
                    col_info["void_strings_perc"] = round((void_count / total_valid) * 100, 2)

        # TODO pattern recognition (regex)
        # TODO % of numerics
        # TODO % of alfab
        # TODO % of UUID
        # TODO % of email/URL

        # TODO time-series data
        # TODO min_date, max_date
        # TODO time_span
        # TODO frequency distribution (day of the week, month)
        # TODO time zone validation

        # TODO % of true
        # TODO % of false
            
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