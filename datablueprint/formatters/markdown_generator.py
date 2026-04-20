import logging
from typing import Dict, Any, List

logger = logging.getLogger("DataBlueprint.Formatter.Markdown")

def generate_aggregated_markdown(metadata_list: List[Dict[str, Any]], folder_name: str) -> str:
    """
    Genera un unico reporte Markdown que contiene un resumen del directorio 
    y los esquemas detallados de cada archivo procesado.
    """
    logger.info("Generando reporte consolidado en formato Markdown...")
    
    md_lines = []
    
    # ==========================================
    # PARTE 1: CABECERA Y RESUMEN DEL DIRECTORIO
    # ==========================================
    md_lines.append("# DataBlueprint: Context Report\n")
    md_lines.append(f"**Target:** `{folder_name}`")
    md_lines.append(f"**Total Files Processed:** {len(metadata_list)}\n")

    md_lines.append("## 1. Data Inventory Summary\n")
    md_lines.append("| File Name | Format | Size (KB) | Rows | Columns | Privacy |")
    md_lines.append("| :--- | :--- | :--- | :--- | :--- | :--- |")

    for meta in metadata_list:
        sys_info = meta.get('system', {})
        struct_info = meta.get('structure', {})
        
        file_name = sys_info.get('file_name', 'Unknown')
        ext = sys_info.get('extension', '').replace('.', '').upper()
        size_kb = round(sys_info.get('size_bytes', 0) / 1024, 2)
        rows = struct_info.get('total_rows', 0)
        cols = struct_info.get('total_columns', 0)
        
        md_lines.append(f"| `{file_name}` | {ext} | {size_kb} | {rows} | {cols} | Masked |")

    md_lines.append("\n---\n")

    # ==========================================
    # PARTE 2: DETALLES DE CADA ARCHIVO
    # ==========================================
    md_lines.append("## 2. Detailed File Blueprints\n")

    for meta in metadata_list:
        sys_info = meta.get("system", {})
        schema_info = meta.get("schema", {})
        sample_info = meta.get("sample", [])
        
        file_name = sys_info.get('file_name', 'Unknown')
        
        md_lines.append(f"### File: `{file_name}`\n")
        
        # 2.A. Detalles del Esquema (Columnas)
        md_lines.append("#### Schema Details")
        md_lines.append("| Column Name | Data Type | Null % | Unique | Min | Max | Mean |")
        md_lines.append("| :--- | :--- | :--- | :--- | :--- | :--- | :--- |")

        for col, details in schema_info.items():
            dtype = details.get("data_type", "Unknown")
            null_pct = details.get("null_percentage", 0.0)
            unique = details.get("unique_values", "-")
            c_min = details.get("min", "-")
            c_max = details.get("max", "-")
            c_mean = details.get("mean", "-")
            
            row = f"| `{col}` | {dtype} | {null_pct}% | {unique} | {c_min} | {c_max} | {c_mean} |"
            md_lines.append(row)

        # 2.B. Muestra de Datos (Sanitizada)
        md_lines.append("\n#### Safe Data Sample")
        md_lines.append("*Note: Sensitive PII data has been masked.*\n")

        if sample_info:
            headers = list(sample_info[0].keys())
            md_lines.append("| " + " | ".join([f"`{h}`" for h in headers]) + " |")
            md_lines.append("|" + "|".join([" :--- "] * len(headers)) + "|")

            for row_dict in sample_info:
                row_values = [str(row_dict.get(h, "")) for h in headers]
                md_lines.append("| " + " | ".join(row_values) + " |")
        else:
            md_lines.append("> No sample data available.")
            
        md_lines.append("\n<br>\n") # Espacio extra entre archivos

    logger.info("Reporte Markdown consolidado generado con exito.")
    return "\n".join(md_lines)