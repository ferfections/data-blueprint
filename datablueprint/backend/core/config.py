import os
from pathlib import Path
from pydantic_settings import BaseSettings, SettingsConfigDict
from typing import List

# Ruta base del proyecto (apunta a la carpeta datablueprint/backend)
BASE_DIR = Path(__file__).resolve().parent.parent

class Settings(BaseSettings):
    PROJECT_NAME: str = "DataBlueprint API"
    VERSION: str = "0.2.0"
    API_V1_STR: str = "/api/v1"
    
    # Orígenes permitidos para CORS (donde vivirá nuestro frontend de Next.js)
    BACKEND_CORS_ORIGINS: List[str] = [
        "http://localhost:3000",
        "http://127.0.0.1:3000"
    ]
    
    # Directorio donde guardaremos los archivos temporales y los blueprints
    WORKSPACE_DIR: Path = BASE_DIR / "workspace"
    
    # Clave para el LLM
    GROQ_API_KEY: str = ""
    
    # Le decimos a pydantic que lea el archivo .env desde la raíz del proyecto
    model_config = SettingsConfigDict(
        env_file=str(BASE_DIR.parent.parent / ".env"), 
        env_ignore_empty=True, 
        extra="ignore"
    )

# Instanciamos los settings para poder importarlos en otros archivos
settings = Settings()