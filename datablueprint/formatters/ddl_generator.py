import logging
from typing import Dict, Any, List

logger = logging.getLogger("DataBlueprint.Formatters.DDL")

# Diccionario de traduccion de Polars a SQL ANSI / PostgreSQL
TYPE_MAPPING = {
    "Int8": "SMALLINT",
    "Int16": "SMALLINT",
    "Int32": "INTEGER",
    "Int64": "BIGINT",
    "Float32": "REAL",
    "Float64": "DOUBLE PRECISION",
    "Decimal": "NUMERIC",
    "String": "VARCHAR",
    "Utf8": "VARCHAR",
    "Categorical": "VARCHAR",
    "Boolean": "BOOLEAN",
    "Date": "DATE",
    "Datetime": "TIMESTAMP"
}

def _map_sql_type(polars_type: str) -> str:
    """Busca el tipo de dato SQL correspondiente. Devuelve VARCHAR por defecto."""
    for key, sql_type in TYPE_MAPPING.items():
        if key.lower() in polars_type.lower():
            return sql_type
    return "VARCHAR"

def _sanitize_name(name: str) -> str:
    """Limpia los nombres de tablas y columnas para que sean validos en SQL."""
    # Reemplaza espacios y caracteres raros por guiones bajos
    clean_name = "".join(c if c.isalnum() else "_" for c in name)
    # Evita que empiece por un numero
    if clean_name and clean_name[0].isdigit():
        clean_name = "col_" + clean_name
    return clean_name.lower()

def generate_sql_ddl(blueprint_data: List[Dict[str, Any]]) -> str:
    """
    Toma la lista de metadatos extraida por el profiler y genera 
    sentencias CREATE TABLE en SQL.
    """
    sql_statements = [
        "-- ==========================================",
        "-- DDL Generado Automaticamente por DataBlueprint",
        "-- ==========================================\n"
    ]

    for dataset in blueprint_data:
        try:
            # Extraemos el nombre base del archivo sin extension para la tabla
            raw_filename = dataset["system"]["file_name"].rsplit('.', 1)[0]
            table_name = _sanitize_name(raw_filename)
            
            total_rows = dataset["structure"].get("total_rows", 0)
            schema = dataset.get("schema", {})
            
            columns_sql = []
            
            for col_name, col_info in schema.items():
                safe_col_name = _sanitize_name(col_name)
                raw_type = col_info.get("data_type", "String")
                sql_type = _map_sql_type(raw_type)
                
                constraints = []
                
                # Regla 1: NOT NULL
                if col_info.get("null_count", 0) == 0 and total_rows > 0:
                    constraints.append("NOT NULL")
                    
                # Regla 2: Inferencia de PRIMARY KEY
                # Si no hay nulos y todos los valores son unicos, es una PK candidata
                unique_vals = col_info.get("unique_values", 0)
                if total_rows > 0 and unique_vals == total_rows and col_info.get("null_count", 0) == 0:
                    # Solo sugerimos PK si se llama id, code, pk, o similar para no equivocarnos con emails, etc.
                    if any(kw in safe_col_name for kw in ["id", "code", "key", "uuid"]):
                        constraints.append("PRIMARY KEY")
                
                constraint_str = " ".join(constraints)
                
                # Construimos la linea de la columna: "    nombre_columna TIPO CONSTRAINTS"
                line = f"    {safe_col_name} {sql_type}"
                if constraint_str:
                    line += f" {constraint_str}"
                
                suggested = col_info.get("suggested_values")
                if suggested:
                    # Convertimos la lista a un texto separado por comas
                    valores_texto = ", ".join([str(v) for v in suggested])
                    line += f" -- Valores reales: {valores_texto}"
                    
                columns_sql.append(line)

                
            create_statement = f"CREATE TABLE {table_name} (\n" + ",\n".join(columns_sql) + "\n);"
            sql_statements.append(create_statement)
            
        except Exception as e:
            logger.error(f"Error generando DDL para {dataset['system'].get('file_name', 'archivo desconocido')}: {str(e)}")

    return "\n\n".join(sql_statements)