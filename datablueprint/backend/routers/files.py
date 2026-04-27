import shutil
import logging
from pathlib import Path
from fastapi import APIRouter, UploadFile, File, HTTPException
from typing import List

from datablueprint.backend.core.config import settings
from datablueprint.backend.models.schemas import BlueprintGenerationResponse, FileMetadataSummary, UploadResponse
from datablueprint.backend.services.profiler_service import generate_blueprint_for_file
from datablueprint.formatters.ddl_generator import generate_sql_ddl
from datablueprint.core.profiler import process_file


logger = logging.getLogger("DataBlueprint.API.Chat")
router = APIRouter()

@router.post("/upload", response_model=UploadResponse)
async def upload_files(files: List[UploadFile] = File(...)):
    """
    Recibe múltiples archivos, los guarda temporalmente y extrae su DDL y perfiles.
    """
    try:
        processed_files = []
        
        # 1. Limpiamos el workspace anterior
        for existing_file in settings.WORKSPACE_DIR.glob("*"):
            existing_file.unlink()
            
        # 2. Procesamos cada archivo
        for file in files:
            content = await file.read()
            
            # PROTECCIÓN: Si el archivo está vacío, lanzamos un 400 controlado
            if not content:
                raise HTTPException(status_code=400, detail=f"El archivo {file.filename} está vacío.")
                
            file_location = settings.WORKSPACE_DIR / file.filename
            with open(file_location, "wb+") as file_object:
                file_object.write(content)
                
            # Extraemos los metadatos
            metadata = process_file(file_location)
            processed_files.append(metadata)
            
        # 3. Generamos el Blueprint
        blueprint_sql = generate_sql_ddl(processed_files)
        context_path = settings.WORKSPACE_DIR / "context_blueprint.sql"
        with open(context_path, "w", encoding="utf-8") as f:
            f.write(blueprint_sql)
            
        return UploadResponse(
            status="success",
            processed_files=processed_files,
            global_context_saved=True
        )

    # NUEVO: Dejamos pasar los errores HTTP controlados (como nuestro 400)
    except HTTPException:
        raise
    # Atrapamos fallos reales del sistema y los marcamos como 500
    except Exception as e:
        logger.error(f"Error procesando archivos: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))