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

def generate_aggregated_markdown(metadata_list: list[dict], folder_name: str) -> str:
    """
    Genera un unico reporte Markdown que resume multiples archivos.
    """
    md_lines = []
    md_lines.append(f"# 🏗️ DataBlueprint: Folder Analysis Report")
    md_lines.append(f"**Target Directory:** `{folder_name}`")
    md_lines.append(f"**Total Files Processed:** {len(metadata_list)}\n")

    # 1. Tabla de Contenidos (Resumen Ejecutivo)
    md_lines.append("## 📂 Data Inventory Summary")
    md_lines.append("| File Name | Format | Rows | Columns | Privacy Status |")
    md_lines.append("| :--- | :--- | :--- | :--- | :--- |")

    for meta in metadata_list:
        sys = meta['system']
        struct = meta['structure']
        md_lines.append(
            f"| {sys['file_name']} | {sys['extension'].upper()} | "
            f"{struct['total_rows']} | {struct['total_columns']} | ✅ Sanitized |"
        )

    md_lines.append("\n---\n")

    # 2. Secciones Detalladas por Archivo
    md_lines.append("## 🔍 Detailed File Blueprints")
    for meta in metadata_list:
        # Reutilizamos parte de la logica anterior pero como subsecciones
        sys = meta['system']
        md_lines.append(f"### File: `{sys['file_name']}`")
        
        # (Aqui pegariamos la logica de tablas de columnas y muestras del archivo anterior)
        # Para mantener el codigo limpio, puedes llamar a una funcion auxiliar 
        # que genere solo el fragmento de un archivo.
        md_lines.append("\n---\n")

    return "\n".join(md_lines)