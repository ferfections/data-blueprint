import json
import logging
import sys
from pathlib import Path
from typing import Dict, Any

logger = logging.getLogger("DataBlueprint.DriftDetector")

def load_blueprint(file_path: Path) -> Dict[str, Any]:
    if not file_path.exists():
        logger.error(f"No se encuentra el archivo: {file_path}")
        sys.exit(1)
        
    with open(file_path, 'r', encoding='utf-8') as f:
        return json.load(f)

def compare_blueprints(old_path: str, new_path: str) -> None:
    logger.info("Iniciando auditoria de Schema Drift...")
    logger.info(f"Contrato base: {old_path}")
    logger.info(f"Nuevos datos:  {new_path}")

    old_bp = load_blueprint(Path(old_path))
    new_bp = load_blueprint(Path(new_path))

    # Convertimos las listas a diccionarios para buscar rapido por nombre de archivo
    old_files = {f['system']['file_name']: f for f in old_bp.get('files', [])}
    new_files = {f['system']['file_name']: f for f in new_bp.get('files', [])}

    drift_detected = False

    # 1. Archivos a nivel de directorio
    missing_files = set(old_files.keys()) - set(new_files.keys())
    added_files = set(new_files.keys()) - set(old_files.keys())

    if missing_files:
        logger.error(f"[ALERTA] Archivos desaparecidos: {', '.join(missing_files)}")
        drift_detected = True
    if added_files:
        logger.warning(f"[INFO] Nuevos archivos detectados (sin contrato previo): {', '.join(added_files)}")

    # 2. Comparacion de columnas y tipos
    common_files = set(old_files.keys()).intersection(set(new_files.keys()))

    for file_name in common_files:
        old_schema = old_files[file_name].get('schema', {})
        new_schema = new_files[file_name].get('schema', {})

        old_cols = set(old_schema.keys())
        new_cols = set(new_schema.keys())

        missing_cols = old_cols - new_cols
        added_cols = new_cols - old_cols

        if missing_cols:
            logger.error(f"[DRIFT] '{file_name}': Columnas eliminadas -> {missing_cols}")
            drift_detected = True

        if added_cols:
            logger.warning(f"[ALERTA] '{file_name}': Columnas nuevas sin perfilar -> {added_cols}")
            drift_detected = True

        # Comparacion de tipos de datos
        common_cols = old_cols.intersection(new_cols)
        for col in common_cols:
            old_type = old_schema[col].get('data_type')
            new_type = new_schema[col].get('data_type')

            if old_type != new_type:
                logger.error(f"[CRITICO] '{file_name}' | '{col}': Mutacion de tipo '{old_type}' a '{new_type}'")
                drift_detected = True

    logger.info("-" * 50)
    if not drift_detected:
        logger.info("✅ VALIDACION EXITOSA: Los datos cumplen el contrato al 100%.")
    else:
        logger.error("❌ FALLO DE CONTRATO: Se requiere intervencion manual para evitar roturas.")
        sys.exit(1) # Devolvemos error al sistema operativo (util para CI/CD)