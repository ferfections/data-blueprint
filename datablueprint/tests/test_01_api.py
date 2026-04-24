from fastapi.testclient import TestClient
from datablueprint.backend.main import app

# Creamos el cliente de pruebas que interactúa con tu API
client = TestClient(app)

def test_health_check_devuelve_200_y_estado_online():
    """
    Verifica que el API está despierto.
    Usamos aserciones resilientes (buscamos claves específicas, no el diccionario entero).
    """
    response = client.get("/health")
    
    # Verificamos código HTTP
    assert response.status_code == 200
    
    # Verificamos estructura del JSON
    data = response.json()
    assert "status" in data
    assert data["status"] == "online"
    assert "project" in data
    assert "version" in data

def test_ruta_no_existente_devuelve_404():
    """Verifica que el sistema maneja bien las rutas que no existen."""
    response = client.get("/api/v1/ruta-inventada-que-no-existe")
    
    assert response.status_code == 404
    assert response.json() == {"detail": "Not Found"}

def test_metodo_http_incorrecto_devuelve_405():
    """
    Verifica que la API protege sus endpoints contra verbos HTTP incorrectos.
    Ejemplo: Intentar hacer un POST a una ruta que solo acepta GET.
    """
    response = client.post("/health")
    
    # 405 significa "Method Not Allowed" (Método no permitido)
    assert response.status_code == 405

def test_documentacion_swagger_esta_accesible():
    """
    Verifica que FastAPI está generando el panel de control gráfico (Swagger)
    y el esquema OpenAPI correctamente.
    """
    # 1. Comprobamos la página web (HTML)
    response_docs = client.get("/docs")
    assert response_docs.status_code == 200
    assert "text/html" in response_docs.headers["content-type"]
    
    # 2. Comprobamos el JSON puro que alimenta a Swagger
    # (Ajustado a la ruta versionada de tu arquitectura)
    response_openapi = client.get("/api/v1/openapi.json")
    assert response_openapi.status_code == 200
    
    data_openapi = response_openapi.json()
    assert "openapi" in data_openapi
    assert "info" in data_openapi