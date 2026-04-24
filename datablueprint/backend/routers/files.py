import shutil
from pathlib import Path
from fastapi import APIRouter, UploadFile, File, HTTPException
from typing import List

from datablueprint.backend.core.config import settings
from datablueprint.backend.models.schemas import BlueprintGenerationResponse, FileMetadataSummary
from datablueprint.backend.services.profiler_service import generate_blueprint_for_file
from datablueprint.formatters.ddl_generator import generate_sql_ddl

router = APIRouter()

@router.post("/upload", response_model=BlueprintGenerationResponse)
async def upload_files(files: List[UploadFile] = File(...)):
    """
    Recibe uno o varios archivos, los guarda en el workspace y genera su Blueprint.
    """
    if not files:
        raise HTTPException(status_code=400, detail="No se enviaron archivos")
        
    processed_summaries = []
    all_metadata = []
    
    for file in files:
        # 1. FastAPI guarda el archivo en el almacén (workspace temporal)
        file_path = settings.WORKSPACE_DIR / file.filename
        
        try:
            with open(file_path, "wb") as buffer:
                shutil.copyfileobj(file.file, buffer)
                
            # 2. FastAPI le pasa el archivo a la "Cocina" (Polars)
            metadata = generate_blueprint_for_file(file_path)
            all_metadata.append(metadata)
            
            # 3. Empaquetamos el resumen para devolvérselo al Frontend (Next.js)
            summary = FileMetadataSummary(
                filename=file.filename,
                extension=file_path.suffix.lower(),
                total_rows=metadata["structure"]["total_rows"],
                total_columns=metadata["structure"]["total_columns"],
                columns_list=metadata["structure"]["columns_list"]
            )
            processed_summaries.append(summary)
            
        except Exception as e:
            # Si un archivo falla, avisamos exactamente de por qué
            raise HTTPException(status_code=500, detail=f"Error procesando {file.filename}: {str(e)}")
            
    # 4. Generamos el esquema DDL de todos los archivos juntos y lo guardamos
    # Esto será el "Contexto" que le enviaremos a la IA más adelante.
    if all_metadata:
        sql_context = generate_sql_ddl(all_metadata)
        sql_path = settings.WORKSPACE_DIR / "context_blueprint.sql"
        with open(sql_path, "w", encoding="utf-8") as f:
            f.write(sql_context)
            
    return BlueprintGenerationResponse(
        message="Blueprints y contexto SQL generados correctamente",
        status="success",
        processed_files=processed_summaries
    )