from pydantic import BaseModel, Field
from typing import List, Dict, Any, Optional

# ==========================================
# ESQUEMAS PARA LA INGESTA DE ARCHIVOS
# ==========================================

class FileMetadataSummary(BaseModel):
    """Resumen de alto nivel del archivo procesado para mostrar en la UI"""
    filename: str
    extension: str
    total_rows: int
    total_columns: int
    columns_list: List[str]

class BlueprintGenerationResponse(BaseModel):
    """Respuesta que se enviará al Frontend cuando se genere el Blueprint"""
    message: str
    status: str
    processed_files: List[FileMetadataSummary]
    # En un entorno de produccion real, podriamos devolver el JSON completo
    # o una URL para descargarlo. Por ahora, un resumen es perfecto.

# ==========================================
# ESQUEMAS PARA EL CHAT
# ==========================================

class ChatMessage(BaseModel):
    """Representa un mensaje individual en el historial del chat"""
    role: str = Field(..., description="Puede ser 'user', 'assistant' o 'system'", pattern="^(user|assistant|system)$")
    content: str = Field(..., description="Contenido del mensaje")

class ChatRequest(BaseModel):
    """Lo que el Frontend nos enviara cuando el usuario haga una pregunta"""
    prompt: str = Field(..., min_length=1, description="La pregunta en lenguaje natural")
    # Historial opcional para dar contexto a la IA de preguntas anteriores
    history: Optional[List[ChatMessage]] = Field(default_factory=list)

class ChatResponse(BaseModel):
    """Lo que responderemos (si no usamos Streaming)"""
    sql_generated: Optional[str] = Field(None, description="El codigo SQL generado por la IA")
    text_response: str = Field(..., description="La explicacion o resultado final")
    # Aquí podríamos añadir una lista de diccionarios si queremos enviar los datos reales 
    # de la tabla para que Next.js pinte una tabla bonita.
    data_sample: Optional[List[Dict[str, Any]]] = None