import argparse
import sys
import logging
from pathlib import Path

# Importaciones de nuestros modulos
from datablueprint.core.csv_profiler import process_csv_with_polars
from datablueprint.security.pii_masker import sanitize_sample
from datablueprint.formatters.markdown_generator import generate_aggregated_markdown

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger("DataBlueprint")

SUPPORTED_EXTENSIONS = {'.csv', '.parquet', '.json', '.jsonl'}

def setup_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="DataBlueprint: Extrae metadatos para contexto de IA.")
    parser.add_argument("input_path", type=str, help="Ruta a la carpeta o archivo a analizar.")
    return parser

def get_files_to_process(target_path: Path) -> list[Path]:
    if not target_path.exists():
        logger.error(f"La ruta especificada no existe: {target_path}")
        sys.exit(1)

    files_to_process = []
    if target_path.is_file():
        if target_path.suffix.lower() in SUPPORTED_EXTENSIONS:
            files_to_process.append(target_path)
    elif target_path.is_dir():
        logger.info(f"Escaneando directorio de entrada: {target_path}")
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
        logger.warning("No se encontraron archivos validos en el directorio especificado.")
        sys.exit(0)

    all_metadata = []
    logger.info(f"Iniciando analisis de {len(files)} archivo(s).")

    for file_path in files:
        ext = file_path.suffix.lower()
        try:
            raw_metadata = None
            if ext == '.csv':
                raw_metadata = process_csv_with_polars(file_path)
            
            if raw_metadata:
                if "sample" in raw_metadata and raw_metadata["sample"]:
                    raw_metadata["sample"] = sanitize_sample(raw_metadata["sample"])
                all_metadata.append(raw_metadata)
                
        except Exception as e:
            logger.error(f"Error procesando {file_path.name}: {str(e)}")

    # Generacion del reporte consolidado en la raiz del proyecto
    if all_metadata:
        # Path(__file__).parent nos da el directorio donde reside main.py (la raiz)
        project_root = Path(__file__).parent
        
        # Usamos el nombre del directorio de entrada para nombrar el reporte
        input_name = target_path.name if target_path.is_dir() else target_path.stem
        report_filename = f"{input_name}_blueprint.md"
        report_path = project_root / report_filename

        # Llamamos al formateador que describe el contenido del directorio
        final_report_md = generate_aggregated_markdown(all_metadata, input_name)

        # Guardamos fisicamente en la raiz
        with open(report_path, "w", encoding="utf-8") as f:
            f.write(final_report_md)
        
        logger.info(f"Reporte del directorio generado con exito en: {report_path}")

if __name__ == "__main__":
    main()