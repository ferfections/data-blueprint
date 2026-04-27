import logging
from fastapi import APIRouter, HTTPException
from datablueprint.backend.core.config import settings
from datablueprint.backend.models.schemas import ChatRequest, ChatResponse
from datablueprint.backend.services.llm_service import generate_sql_and_answer
from datablueprint.backend.services.duckdb_service import execute_local_query

logger = logging.getLogger("DataBlueprint.API.Chat")
router = APIRouter()

@router.post("/", response_model=ChatResponse)
async def chat_with_data(request: ChatRequest):
    """
    Recibe una pregunta del usuario, busca el contexto (Blueprint),
    pide a la IA que genere SQL, lo ejecuta localmente y devuelve la respuesta.
    """
    try:
        # 1. Recuperar el Contexto (El Blueprint SQL que generamos en el upload)
        context_path = settings.WORKSPACE_DIR / "context_blueprint.sql"
        if not context_path.exists():
             raise HTTPException(
                 status_code=400, 
                 detail="No hay contexto disponible. Por favor, sube archivos primero."
             )
             
        with open(context_path, "r", encoding="utf-8") as f:
            ddl_context = f.read()

        # 2. Enviar a Groq (Traducir Texto -> SQL)
        logger.info(f"Pregunta del usuario: {request.prompt}")
        llm_result = generate_sql_and_answer(request.prompt, ddl_context)
        
        # Extraemos las dos partes que nos prometió la IA en su JSON
        generated_sql = llm_result.get("query_sql")
        text_explanation = llm_result.get("text_response")
        
        if not generated_sql:
             raise ValueError("La IA no generó código SQL válido.")

        # 3. Ejecutar el SQL localmente con DuckDB
        logger.info(f"SQL Generado por la IA:\n{generated_sql}")
        query_results = execute_local_query(generated_sql)
        
        # 4. Formatear la respuesta final para el Frontend
        # (Opcional: Podríamos inyectar los valores reales en el texto aquí)
        final_response = text_explanation
        
        # Si la consulta devuelve un solo valor (ej: SUM), lo añadimos al texto para hacerlo más claro
        if len(query_results) == 1 and len(query_results[0]) == 1:
            valor_unico = list(query_results[0].values())[0]
            final_response += f"\n\n**Resultado:** {valor_unico}"
            
        return ChatResponse(
            sql_generated=generated_sql,
            text_response=final_response,
            data_sample=query_results[:10] # Devolvemos las primeras 10 filas por si Next.js quiere pintar una tabla
        )

    except Exception as e:
        logger.error(f"Error en el flujo del chat: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))