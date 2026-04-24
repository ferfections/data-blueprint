import json
import logging
from groq import Groq
from datablueprint.backend.core.config import settings

logger = logging.getLogger("DataBlueprint.API.LLM")

# Inicializamos el cliente usando la API Key de nuestro .env
client = Groq(api_key=settings.GROQ_API_KEY)

def generate_sql_and_answer(user_prompt: str, ddl_context: str) -> dict:
    """
    Envía la pregunta y el esquema a la IA de Groq (Llama 3), 
    forzando una respuesta estricta en formato JSON.
    """

    system_prompt = f"""Eres un Ingeniero de Datos experto.
    Tu objetivo es traducir preguntas de lenguaje natural a SQL (dialecto DuckDB).

    REGLAS ESTRICTAS DE FORMATO:
    1. Responde ÚNICAMENTE con un objeto JSON válido. Nada de saludos ni texto adicional.
    2. El JSON debe tener exactamente estas dos claves:
    - "query_sql": El código SQL ejecutable en DuckDB.
    - "text_response": Una respuesta amigable al usuario.

    REGLAS DE MAPPING SEMÁNTICO (IMPORTANTE):
    - Si el usuario menciona un estado (ej. 'pagado'), busca el valor equivalente más lógico en el esquema o asume su traducción (ej. 'paid', 'completed').
    - Si el usuario menciona un país en español, tradúcelo al idioma de los datos (ej. 'España' -> 'Spain').
    - SIEMPRE usa LIKE o ILIKE para comparaciones de texto en el SQL si no estás seguro del valor exacto.

    CONTEXTO DE DATOS (ESQUEMA DDL):
    {ddl_context}
    """
    

    try:
        logger.info("Enviando contexto y pregunta a Groq...")
        
        response = client.chat.completions.create(
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt}
            ],
            model="llama-3.3-70b-versatile", 
            temperature=0.1,        # Temperatura casi a 0 para máxima precisión matemática
            response_format={"type": "json_object"} # ¡LA MAGIA! Fuerza a devolver un JSON parseable
        )
        
        # Extraemos el texto de la respuesta y lo convertimos a un diccionario de Python
        raw_content = response.choices[0].message.content
        result_dict = json.loads(raw_content)
        
        return result_dict
        
    except Exception as e:
        logger.error(f"Error conectando con Groq: {str(e)}")
        raise ValueError(f"Fallo en la generacion de la IA: {str(e)}")