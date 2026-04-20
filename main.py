import argparse
import sys
import logging
import pprint

from pathlib import Path

from datablueprint.core.csv_profiler import process_csv_with_polars
from datablueprint.formatters.markdown_generator import generate_markdown_report
from datablueprint.security.pii_masker import sanitize_sample

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
        
        try:
            raw_metadata = None
            
            if ext == '.csv':
                raw_metadata = process_csv_with_polars(file_path)
            elif ext == '.parquet':
                logger.info("  -> [Modulo Parquet pendiente de implementacion]")
            else:
                logger.info(f"  -> [Modulo para {ext} pendiente de implementacion]")

            if raw_metadata:
                # --- NUEVA CAPA DE SEGURIDAD ---
                # Sanitizamos la muestra antes de generar el informe
                if "sample" in raw_metadata and raw_metadata["sample"]:
                    logger.info("Aplicando reglas de seguridad PII a la muestra de datos...")
                    raw_metadata["sample"] = sanitize_sample(raw_metadata["sample"])
                # -------------------------------

                # Generamos el texto Markdown
                reporte_final = generate_markdown_report(raw_metadata)
                
                # Guardamos el archivo fisico
                path_generado = save_report(reporte_final, file_path)
                logger.info(f"Reporte seguro creado en: {path_generado}")

        except Exception as e:
            logger.error(f"Error procesando {file_path.name}: {str(e)}", exc_info=False)

    logger.info("Proceso completado.")

if __name__ == "__main__":
    main()