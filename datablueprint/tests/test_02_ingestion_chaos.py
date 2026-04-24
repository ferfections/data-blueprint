import io
from fastapi.testclient import TestClient
from datablueprint.backend.main import app

client = TestClient(app)

def test_ingestion_dataset_del_caos():
    caos_csv = """id_cliente, Nombre Raro €! , precio_falso, fecha_compra, status
1,Zapatos,15.50,2023-01-01,paid
2,Camisa,,01/02/2023,pending
3,,Esto_no_es_numero,2023-03-01,
4,Gafas,99.99,2023-13-45,paid
"""
    files = {"files": ("caos_total.csv", io.BytesIO(caos_csv.encode("utf-8")), "text/csv")}
    response = client.post("/api/v1/files/upload", files=files)
    
    assert response.status_code == 200
    data = response.json()
    assert data["status"] == "success"
    
    processed = data["processed_files"][0]
    
    # CORRECCIÓN: Buscamos dentro de la clave 'structure'
    assert processed["structure"]["total_rows"] == 4
    assert processed["structure"]["total_columns"] == 5
    assert " Nombre Raro €! " in processed["structure"]["columns_list"]

def test_ingestion_archivo_vacio():
    files = {"files": ("vacio.csv", io.BytesIO(b""), "text/csv")}
    response = client.post("/api/v1/files/upload", files=files)
    
    # CORRECCIÓN: Ahora esperamos un 400 Bad Request, no un 500.
    assert response.status_code == 400
    assert "está vacío" in response.json()["detail"]

def test_ingestion_multiples_archivos():
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
    # CORRECCIÓN: Buscamos dentro de la clave 'system'
    assert data["processed_files"][0]["system"]["file_name"] == "clientes.csv"