import argparse
import sys
import logging
import pprint
import os

from datetime import datetime
from pathlib import Path

from datablueprint.core.csv_profiler import process_csv_with_polars
from datablueprint.formatters.markdown_generator import generate_aggregated_markdown
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
        logger.warning("No se encontraron archivos validos.")
        sys.exit(0)

    # Lista para acumular todos los metadatos
    all_metadata = []

    logger.info(f"Iniciando analisis de carpeta: {target_path.name}")

    for file_path in files:
        ext = file_path.suffix.lower()
        try:
            raw_metadata = None
            if ext == '.csv':
                raw_metadata = process_csv_with_polars(file_path)
            
            if raw_metadata:
                # Sanitizar muestra
                if "sample" in raw_metadata:
                    raw_metadata["sample"] = sanitize_sample(raw_metadata["sample"])
                
                all_metadata.append(raw_metadata)
                logger.info(f"Metadatos extraidos de {file_path.name}")

        except Exception as e:
            logger.error(f"Error en {file_path.name}: {str(e)}")

    # Generar reporte unico si hay datos
    if all_metadata:
        # 1. Crear carpeta de reportes en la raiz del proyecto
        # Path(__file__).parent.parent asume que main.py esta en la raiz o cerca
        project_root = Path(__file__).parent
        reports_dir = project_root / "data-blueprint-reports"
        reports_dir.mkdir(exist_ok=True)

        # 2. Generar nombre intuitivo (basado en la carpeta o timestamp)
        folder_name = target_path.name if target_path.is_dir() else "single_file"
        timestamp = datetime.now().strftime("%Y%m%d_%H%M")
        report_filename = f"Blueprint_{folder_name}_{timestamp}.md"
        report_path = reports_dir / report_filename

        # 3. Llamar al nuevo formateador de agregacion
        final_report_md = generate_aggregated_markdown(all_metadata, folder_name)

        with open(report_path, "w", encoding="utf-8") as f:
            f.write(final_report_md)
        
        logger.info(f"Reporte consolidado generado con exito en: {report_path}")

if __name__ == "__main__":
    main()