import logging
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from contextlib import asynccontextmanager

from datablueprint.backend.core.config import settings

# Configuración del logger para ver qué pasa en la consola
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger("DataBlueprint.API")

@asynccontextmanager
async def lifespan(app: FastAPI):
    """
    Este bloque se ejecuta una vez cuando el servidor arranca.
    Es el lugar ideal para inicializar cosas (como carpetas).
    """
    logger.info("Iniciando DataBlueprint API...")
    
    # Aseguramos que la carpeta workspace existe antes de recibir archivos
    settings.WORKSPACE_DIR.mkdir(parents=True, exist_ok=True)
    logger.info(f"Directorio Workspace listo en: {settings.WORKSPACE_DIR}")
    
    yield # El servidor se queda escuchando aquí
    
    # Lógica que se ejecuta al apagar el servidor (Ctrl+C)
    logger.info("Apagando el motor DataBlueprint...")

# Creamos la instancia principal de FastAPI
app = FastAPI(
    title=settings.PROJECT_NAME,
    version=settings.VERSION,
    openapi_url=f"{settings.API_V1_STR}/openapi.json",
    lifespan=lifespan
)

# Configuramos CORS para permitir peticiones desde el navegador (Next.js)
if settings.BACKEND_CORS_ORIGINS:
    app.add_middleware(
        CORSMiddleware,
        allow_origins=settings.BACKEND_CORS_ORIGINS,
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )

# Un endpoint de prueba para ver si el servidor está vivo
@app.get("/health", tags=["System"])
async def health_check():
    return {
        "status": "online", 
        "project": settings.PROJECT_NAME, 
        "version": settings.VERSION
    }