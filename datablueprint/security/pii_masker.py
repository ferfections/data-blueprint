import re
import logging
from typing import Any, Dict, List

logger = logging.getLogger("DataBlueprint.Security")

# Patrones comunes de PII (Personally Identifiable Information)
PII_PATTERNS = {
    "email": r'[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+',
    "phone": r'\+?\d{1,3}?[-.\s]?\(?\d{1,3}?\)?[-.\s]?\d{3,4}[-.\s]?\d{4}',
    "dni": r'\d{8}[A-Z]'
}

def mask_text(text: str) -> str:
    """Aplica mascaras a un texto si coincide con patrones sensibles."""
    if not isinstance(text, str):
        return text
        
    for label, pattern in PII_PATTERNS.items():
        if re.search(pattern, text):
            text = re.sub(pattern, f"[{label.upper()}_MASKED]", text)
    return text

def sanitize_sample(sample_data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Itera sobre la muestra de datos y aplica mascaras a cada valor 
    de tipo texto que parezca sensible.
    """
    logger.info("Iniciando sanitizacion de la muestra de datos...")
    sanitized_list = []
    
    for row in sample_data:
        new_row = {}
        for col, value in row.items():
            if isinstance(value, str):
                new_row[col] = mask_text(value)
            else:
                new_row[col] = value
        sanitized_list.append(new_row)
        
    return sanitized_list