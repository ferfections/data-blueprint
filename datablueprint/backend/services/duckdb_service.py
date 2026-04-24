import duckdb
import logging
from pathlib import Path
from datablueprint.backend.core.config import settings

logger = logging.getLogger("DataBlueprint.API.DuckDB")

def execute_local_query(sql_query: str) -> list:
    try:
        # Iniciamos conexión
        with duckdb.connect(database=':memory:') as con:
            # 1. ESCANEO Y REGISTRO AUTOMÁTICO
            # Buscamos todos los archivos en el workspace y los registramos como vistas
            for file_path in settings.WORKSPACE_DIR.glob("*"):
                if file_path.suffix in ['.csv', '.parquet', '.json']:
                    table_name = file_path.stem # 'order_items'
                    # Registramos la vista para que el SQL "SELECT ... FROM order_items" funcione
                    con.execute(f"CREATE VIEW IF NOT EXISTS {table_name} AS SELECT * FROM '{file_path}'")
                    logger.info(f"Tabla registrada: {table_name}")

            # 2. EJECUCIÓN DE LA CONSULTA
            result_df = con.execute(sql_query).df()
            result_df = result_df.replace({float('nan'): None})
            return result_df.to_dict(orient='records')
            
    except Exception as e:
        logger.error(f"Error en DuckDB: {str(e)}")
        raise e