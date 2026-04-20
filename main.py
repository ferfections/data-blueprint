import argparse
import sys
import logging
import pprint

from pathlib import Path

from datablueprint.core.csv_profiler import process_csv_with_polars
from datablueprint.formatters.markdown_generator import generate_markdown_report

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

def save_report(content: str, original_path: Path, extension: str = "md") -> Path:
    """
    Guarda el contenido generado en un archivo fisico.
    El nombre sera: nombre_original_blueprint.extension
    """
    report_name = f"{original_path.stem}_blueprint.{extension}"
    # Guardamos el reporte en la misma carpeta que el archivo original
    report_path = original_path.parent / report_name
    
    with open(report_path, "w", encoding="utf-8") as f:
        f.write(content)
    
    return report_path

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
                raw_metadata = process_csv_with_polars(file_path)
                # Temporalmente, para ver que funciona, imprimimos el diccionario generado:
                pprint.pprint(raw_metadata)
                
            elif ext == '.parquet':
                # raw_metadata = process_parquet(file_path)
                logger.info("  -> [Modulo Parquet pendiente de implementacion]")
                
            else:
                logger.info(f"  -> [Modulo para {ext} pendiente de implementacion]")

            reporte_final = generate_markdown_report(raw_metadata)

            print(reporte_final)

            path_generado = save_report(reporte_final, file_path)
            logger.info(f"Reporte fisico creado en: {path_generado}")
            # generate_json(raw_metadata)

        except Exception as e:
            # Si un archivo falla, registramos el error pero el bucle continua
            logger.error(f"Error procesando {file_path.name}: {str(e)}", exc_info=False)

    logger.info("Proceso completado.")

if __name__ == "__main__":
    main()