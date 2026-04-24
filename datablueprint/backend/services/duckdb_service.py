import duckdb
import logging
from typing import Dict, Any, List

logger = logging.getLogger("DataBlueprint.API.DuckDB")

def execute_local_query(sql_query: str) -> List[Dict[str, Any]]:
    """
    Ejecuta una consulta SQL en memoria utilizando DuckDB.
    DuckDB es capaz de leer archivos directamente desde el disco (workspace/)
    si el SQL contiene rutas como: SELECT * FROM 'workspace/archivo.csv'
    """
    try:
        logger.info(f"Ejecutando SQL en DuckDB:\n{sql_query}")
        
        # Conectamos a una base de datos temporal en memoria
        with duckdb.connect(database=':memory:') as con:
            # Ejecutamos la consulta y transformamos el resultado en un DataFrame
            # y luego en una lista de diccionarios (facil de enviar por API)
            result_df = con.execute(sql_query).df()
            
            # Reemplazamos posibles valores NaN o NaT por None para que JSON no se queje
            result_df = result_df.replace({float('nan'): None})
            
            return result_df.to_dict(orient='records')
            
    except Exception as e:
        logger.error(f"Error ejecutando consulta en DuckDB: {str(e)}")
        raise ValueError(f"Fallo en la ejecucion SQL: {str(e)}")