# Verifica que el servidor (FastAPI) arranca y responde correctamente
from fastapi.testclient import TestClient
from datablueprint.backend.main import app

# Creamos un cliente falso que interactúa con tu API
client = TestClient(app)

def test_health_check_funciona_correctamente():
    """Prueba que el camarero está despierto y respondiendo"""
    
    # 1. ACCIÓN: Hacemos una petición GET a la ruta /health
    response = client.get("/health")
    
    # 2. VALIDACIÓN: Comprobamos el código HTTP (200 significa OK)
    assert response.status_code == 200
    
    # 3. VALIDACIÓN: Comprobamos que el JSON devuelto es exactamente el esperado
    # 3. VALIDACIÓN: Comprobamos que la clave status es online (ignoramos el resto)
    data = response.json()
    assert data["status"] == "online"
    assert "project" in data # Confirmamos que también envía la info del proyecto

def test_ruta_no_existente_da_error_404():
    """Prueba que el sistema maneja bien las rutas que no existen"""
    response = client.get("/ruta-inventada")
    assert response.status_code == 404