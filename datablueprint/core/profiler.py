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
        
        # Base de metadatos universal para todas las columnas
        col_info = {
            "data_type": dtype_str,
            "null_count": null_count,
            "null_percentage": round((null_count / rows * 100), 2) if rows > 0 else 0.0,
            "unique_values": series.n_unique(),
            "memory_bytes": series.estimated_size() # Resuelto el TODO de uso de memoria
        }
        
        # ==========================================
        # DATOS NUMERICOS
        # ==========================================
        if series.dtype in pl.NUMERIC_DTYPES:
            valid_series = series.drop_nulls()
            total_valid = valid_series.len()

            if total_valid > 0:
                col_info["min"] = round(valid_series.min(), 4)
                col_info["max"] = round(valid_series.max(), 4)
                col_info["mean"] = round(valid_series.mean(), 4)

                col_info["std_dev"] = round(valid_series.std(), 4) if total_valid > 1 else 0.0
                col_info["variance"] = round(valid_series.var(), 4) if total_valid > 1 else 0.0

                try:
                    col_info["p25"] = round(valid_series.quantile(0.25), 4)
                    col_info["p50"] = round(valid_series.median(), 4)
                    col_info["p75"] = round(valid_series.quantile(0.75), 4)
                    col_info["p95"] = round(valid_series.quantile(0.95), 4)
                except Exception:
                    pass # Evita fallos si la distribucion es imposible de cuantilar

                zeros_count = (valid_series == 0).sum()
                negatives_count = (valid_series < 0).sum()
                
                col_info["zeros_perc"] = round((zeros_count / total_valid) * 100, 2)
                col_info["negatives_perc"] = round((negatives_count / total_valid) * 100, 2)

                try:
                    hist_df = valid_series.hist(bin_count=5)
                    count_col = [c for c in hist_df.columns if "count" in c.lower()][0]
                    bp_col = "break_point"
                    
                    hist_parts = []
                    prev_bp = col_info["min"]
                    
                    for row in hist_df.iter_rows(named=True):
                        current_bp = row[bp_col]
                        count = row[count_col]
                        perc = round((count / total_valid) * 100, 1)
                        if perc > 0:
                            hist_parts.append(f"[{round(prev_bp, 2)}-{round(current_bp, 2)}: {perc}%]")
                        prev_bp = current_bp
                        
                    col_info["histogram"] = ", ".join(hist_parts)
                except Exception:
                    col_info["histogram"] = "N/A"

        # ==========================================
        # DATOS DE TEXTO (Strings y Categoricos)
        # ==========================================
        elif series.dtype in [pl.String, pl.Categorical]:
            valid_series = series.drop_nulls()
            total_valid = valid_series.len()

            if total_valid > 0:
                try:
                    top_counts = valid_series.value_counts(sort=True).head(3)
                    count_col = "count" if "count" in top_counts.columns else "counts"
                    val_col = top_counts.columns[0]
                    
                    top_parts = []
                    for row in top_counts.iter_rows(named=True):
                        val = row[val_col]
                        cnt = row[count_col]
                        perc = round((cnt / total_valid) * 100, 1)
                        top_parts.append(f'"{val}" ({perc}%)')
                    
                    col_info["top_3_values"] = f"[{', '.join(top_parts)}]"
                except Exception:
                    col_info["top_3_values"] = "N/A"

                if series.dtype == pl.String:
                    lengths = valid_series.str.len_chars()
                    col_info["min_length"] = lengths.min()
                    col_info["max_length"] = lengths.max()
                    col_info["mean_length"] = round(lengths.mean(), 2) if lengths.mean() is not None else 0

                    empty_count = (valid_series == "").sum()
                    void_count = ((valid_series != "") & (valid_series.str.strip_chars() == "")).sum()

                    col_info["empty_strings_perc"] = round((empty_count / total_valid) * 100, 2)
                    col_info["void_strings_perc"] = round((void_count / total_valid) * 100, 2)

                    REGEX_NUMERIC = r"^\d+$"
                    REGEX_ALPHA = r"^[a-zA-ZáéíóúÁÉÍÓÚñÑ\s]+$" 
                    REGEX_UUID = r"^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[1-5][0-9a-fA-F]{3}-[89abAB][0-9a-fA-F]{3}-[0-9a-fA-F]{12}$"
                    REGEX_EMAIL = r"^[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+$"
                    REGEX_URL = r"^https?:\/\/(?:www\.)?[-a-zA-Z0-9@:%._\+~#=]{1,256}\.[a-zA-Z0-9()]{1,6}\b(?:[-a-zA-Z0-9()@:%_\+.~#?&\/=]*)$"

                    num_count = valid_series.str.contains(REGEX_NUMERIC).sum()
                    alpha_count = valid_series.str.contains(REGEX_ALPHA).sum()
                    uuid_count = valid_series.str.contains(REGEX_UUID).sum()
                    email_count = valid_series.str.contains(REGEX_EMAIL).sum()
                    url_count = valid_series.str.contains(REGEX_URL).sum()

                    perc_num = round((num_count / total_valid) * 100, 2)
                    perc_alpha = round((alpha_count / total_valid) * 100, 2)
                    perc_uuid = round((uuid_count / total_valid) * 100, 2)
                    perc_email = round((email_count / total_valid) * 100, 2)
                    perc_url = round((url_count / total_valid) * 100, 2)

                    patterns_found = []
                    if perc_num > 0: patterns_found.append(f"Numeric Only: {perc_num}%")
                    if perc_alpha > 0: patterns_found.append(f"Alpha Only: {perc_alpha}%")
                    if perc_uuid > 0: patterns_found.append(f"UUID: {perc_uuid}%")
                    if perc_email > 0: patterns_found.append(f"Email: {perc_email}%")
                    if perc_url > 0: patterns_found.append(f"URL: {perc_url}%")

                    if patterns_found:
                        col_info["semantic_patterns"] = ", ".join(patterns_found)

        # ==========================================
        # DATOS TEMPORALES (Fechas y Tiempos)
        # ==========================================
        elif series.dtype in [pl.Date, pl.Datetime]:
            valid_series = series.drop_nulls()
            total_valid = valid_series.len()

            if total_valid > 0:
                min_date = valid_series.min()
                max_date = valid_series.max()

                col_info["min_date"] = str(min_date)
                col_info["max_date"] = str(max_date)

                try:
                    span = max_date - min_date
                    col_info["time_span_days"] = span.days
                except Exception:
                    col_info["time_span_days"] = "N/A"

                try:
                    top_weekday_df = valid_series.dt.weekday().value_counts(sort=True).head(1)
                    val_col = top_weekday_df.columns[0]
                    count_col = top_weekday_df.columns[1] if len(top_weekday_df.columns) > 1 else "count"
                    
                    weekday_num = top_weekday_df[val_col][0]
                    weekday_perc = round((top_weekday_df[count_col][0] / total_valid) * 100, 1)
                    
                    weekdays_map = {1:"Lunes", 2:"Martes", 3:"Miercoles", 4:"Jueves", 5:"Viernes", 6:"Sabado", 7:"Domingo"}
                    col_info["top_weekday"] = f"{weekdays_map.get(weekday_num, str(weekday_num))} ({weekday_perc}%)"

                    top_month_df = valid_series.dt.month().value_counts(sort=True).head(1)
                    m_val_col = top_month_df.columns[0]
                    m_count_col = top_month_df.columns[1] if len(top_month_df.columns) > 1 else "count"
                    
                    month_num = top_month_df[m_val_col][0]
                    month_perc = round((top_month_df[m_count_col][0] / total_valid) * 100, 1)
                    col_info["top_month"] = f"Mes {month_num} ({month_perc}%)"
                    
                except Exception:
                    col_info["top_weekday"] = "N/A"
                    col_info["top_month"] = "N/A"

                if series.dtype == pl.Datetime:
                    tz = series.dtype.time_zone
                    col_info["timezone"] = tz if tz is not None else "Naive (Sin timezone)"

        # ==========================================
        # DATOS BOOLEANOS (Verdadero / Falso)
        # ==========================================
        elif series.dtype == pl.Boolean:
            valid_series = series.drop_nulls()
            total_valid = valid_series.len()

            if total_valid > 0:
                true_count = valid_series.sum()
                false_count = total_valid - true_count

                col_info["true_perc"] = round((true_count / total_valid) * 100, 2)
                col_info["false_perc"] = round((false_count / total_valid) * 100, 2)
            
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