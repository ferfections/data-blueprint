# El "Dataset del Caos": estresa a Polars con archivos corruptos y sucios
import io
from fastapi.testclient import TestClient
from datablueprint.backend.main import app

client = TestClient(app)

def test_ingestion_dataset_del_caos():
    """
    Simula la subida de un CSV diseñado específicamente para romper parsers:
    - Nombres de columnas con espacios al principio/final, tildes y símbolos.
    - Datos faltantes (nulos) asimétricos (comas seguidas).
    - Tipos de datos mezclados (letras en columnas donde hay números).
    - Fechas inválidas que romperían un parseo estricto de datetime.
    """
    # Fíjate en los espacios intencionados y la falta de valores en la fila 3
    caos_csv = """id_cliente, Nombre Raro €! , precio_falso, fecha_compra, status
    1,Zapatos,15.50,2023-01-01,paid
    2,Camisa,,01/02/2023,pending
    3,,Esto_no_es_numero,2023-03-01,
    4,Gafas,99.99,2023-13-45,paid
    """
    
    # Empaquetamos el string en la memoria RAM simulando ser un archivo físico
    files = {
        "files": ("caos_total.csv", io.BytesIO(caos_csv.encode("utf-8")), "text/csv")
    }
    
    # 1. ACCIÓN: Disparamos el archivo al endpoint
    response = client.post("/api/v1/files/upload", files=files)
    
    # 2. VALIDACIÓN: ¡El servidor sobrevivió! No hay Error 500
    assert response.status_code == 200
    
    data = response.json()
    assert data["status"] == "success"
    
    # 3. VALIDACIÓN PROFUNDA: Analizamos cómo lo entendió Polars
    processed = data["processed_files"][0]
    
    # Comprobamos que no se ha comido filas ni columnas a pesar de los errores
    assert processed["total_rows"] == 4
    assert processed["total_columns"] == 5
    
    # Comprobamos que respetó el nombre horrible de la columna sin crashear
    columnas_leidas = processed["columns_list"]
    assert " Nombre Raro €! " in columnas_leidas


def test_ingestion_archivo_vacio():
    """
    Comprueba qué pasa si el usuario sube un archivo completamente en blanco.
    Un buen sistema no debe dar un Internal Server Error (500), sino procesarlo 
    como vacío o devolver un error controlado (400).
    """
    files = {
        "files": ("vacio.csv", io.BytesIO(b""), "text/csv")
    }
    
    response = client.post("/api/v1/files/upload", files=files)
    
    # Verificamos que el servidor haya manejado la excepción gracefully
    # (Si Polars lanza un NoDataError, FastAPI debería atraparlo y devolver un error HTTP o procesarlo vacío, pero NUNCA un 500)
    assert response.status_code != 500


def test_ingestion_multiples_archivos():
    """
    Verifica que el sistema puede procesar varios archivos a la vez, 
    algo crucial para cuando conectemos el frontend en Next.js.
    """
    csv_1 = "id,nombre\n1,Alpha"
    csv_2 = "id,ventas\n1,100"
    
    files = [
        ("files", ("clientes.csv", io.BytesIO(csv_1.encode("utf-8")), "text/csv")),
        ("files", ("ventas.csv", io.BytesIO(csv_2.encode("utf-8")), "text/csv"))
    ]
    
    response = client.post("/api/v1/files/upload", files=files)
    assert response.status_code == 200
    
    data = response.json()
    assert len(data["processed_files"]) == 2
    assert data["processed_files"][0]["file_name"] == "clientes.csv"
    assert data["processed_files"][1]["file_name"] == "ventas.csv"