import argparse
import sys
import logging
from pathlib import Path

# Configuracion del Logger (Nivel INFO por defecto)
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger("DataBlueprint")

# Extensiones soportadas
SUPPORTED_EXTENSIONS = {'.csv', '.parquet', '.json', '.jsonl'}
def setup_parser() -> argparse.ArgumentParser:
    """Configura y devuelve el parser de argumentos de la linea de comandos."""
    parser = argparse.ArgumentParser(
        description="DataBlueprint: Extrae metadatos de archivos para contexto de IA."
    )
    parser.add_argument(
        "input_path", 
        type=str, 
        help="Ruta al archivo o carpeta a analizar."
    )
    return parser

def get_files_to_process(target_path: Path) -> list[Path]:
    """Valida la ruta y extrae la lista de archivos soportados."""
    if not target_path.exists():
        logger.error(f"La ruta especificada no existe: {target_path}")
        sys.exit(1)

    files_to_process = []

    if target_path.is_file():
        if target_path.suffix.lower() in SUPPORTED_EXTENSIONS:
            files_to_process.append(target_path)
        else:
            logger.warning(f"Formato no soportado para el archivo: {target_path.name}")
            
    elif target_path.is_dir():
        logger.info(f"Escaneando directorio: {target_path}")
        for file_path in target_path.iterdir():
            if file_path.is_file() and file_path.suffix.lower() in SUPPORTED_EXTENSIONS:
                files_to_process.append(file_path)
                
    return files_to_process

def main() -> None:
    parser = setup_parser()
    args = parser.parse_args()
    target_path = Path(args.input_path)

    files = get_files_to_process(target_path)

    if not files:
        logger.warning("No se encontraron archivos validos para procesar.")
        sys.exit(0)

    logger.info(f"Iniciando procesamiento de {len(files)} archivo(s).")

    for file_path in files:
        ext = file_path.suffix.lower()
        logger.info(f"Procesando archivo: {file_path.name} [{ext}]")
        
        # Bloque Try/Except para aislar errores por archivo
        try:
            if ext == '.csv':
                # raw_metadata = process_csv_with_polars(file_path)
                logger.info("  -> [Modulo CSV pendiente de implementacion]")
                
            elif ext == '.parquet':
                # raw_metadata = process_parquet(file_path)
                logger.info("  -> [Modulo Parquet pendiente de implementacion]")
                
            else:
                logger.info(f"  -> [Modulo para {ext} pendiente de implementacion]")

            # generate_markdown(raw_metadata)
            # generate_json(raw_metadata)

        except Exception as e:
            # Si un archivo falla, registramos el error pero el bucle continua
            logger.error(f"Error procesando {file_path.name}: {str(e)}", exc_info=False)

    logger.info("Proceso completado.")

