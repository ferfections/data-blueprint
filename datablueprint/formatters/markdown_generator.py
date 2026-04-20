import logging
from typing import Dict, Any

logger = logging.getLogger("DataBlueprint.Formatter.Markdown")

def generate_markdown_report(metadata: Dict[str, Any]) -> str:
    """
    Convierte el diccionario de metadatos extraido por el profiler 
    en un documento Markdown estructurado y optimizado para LLMs.
    """
    logger.info("Generando reporte en formato Markdown...")
    
    sys_info = metadata.get("system", {})
    struct_info = metadata.get("structure", {})
    schema_info = metadata.get("schema", {})
    sample_info = metadata.get("sample", [])

    md_lines = []
    
    # 1. Cabecera y Metadatos de Sistema
    md_lines.append("# DataBlueprint: Context Report\n")
    md_lines.append(f"**File:** `{sys_info.get('file_path', 'Unknown')}`")
    
    extension = sys_info.get('extension', '').replace('.', '').upper()
    size_kb = round(sys_info.get('size_bytes', 0) / 1024, 2)
    md_lines.append(f"**Format:** {extension} | **Size:** {size_kb} KB")
    md_lines.append(f"**Rows:** {struct_info.get('total_rows', 0)} | **Columns:** {struct_info.get('total_columns', 0)}\n")

    # 2. Detalles del Esquema
    md_lines.append("## Schema Details\n")
    md_lines.append("| Column Name | Data Type | Null % | Unique | Min | Max | Mean |")
    md_lines.append("| :--- | :--- | :--- | :--- | :--- | :--- | :--- |")

    for col, details in schema_info.items():
        dtype = details.get("data_type", "Unknown")
        null_pct = details.get("null_percentage", 0.0)
        unique = details.get("unique_values", "-")
        
        # Obtenemos estadisticas si existen, sino ponemos un guion
        c_min = details.get("min", "-")
        c_max = details.get("max", "-")
        c_mean = details.get("mean", "-")
        
        # Formateamos la fila de la tabla
        row = f"| `{col}` | {dtype} | {null_pct}% | {unique} | {c_min} | {c_max} | {c_mean} |"
        md_lines.append(row)

    # 3. Muestra de Datos
    md_lines.append("\n## Data Sample\n")
    md_lines.append("*Note: This is a raw sample. PII masking has not been applied yet.*\n")

    if sample_info:
        # Extraemos las cabeceras de la primera fila del sample
        headers = list(sample_info[0].keys())
        
        # Creamos la cabecera de la tabla Markdown
        md_lines.append("| " + " | ".join([f"`{h}`" for h in headers]) + " |")
        md_lines.append("|" + "|".join([" :--- "] * len(headers)) + "|")

        # Rellenamos los datos
        for row_dict in sample_info:
            # Convertimos todos los valores a string para evitar errores con nulos (None)
            row_values = [str(row_dict.get(h, "")) for h in headers]
            md_lines.append("| " + " | ".join(row_values) + " |")
    else:
        md_lines.append("> No sample data available or file is empty.")

    logger.info("Reporte Markdown generado con exito.")
    
    # Unimos todas las lineas con saltos de linea
    return "\n".join(md_lines)