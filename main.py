import argparse
import sys
import logging
from pathlib import Path

# Importaciones de nuestros modulos
from datablueprint.core.profiler import process_csv, process_parquet, process_json
from datablueprint.security.pii_masker import sanitize_sample
from datablueprint.formatters.markdown_generator import generate_aggregated_markdown
from datablueprint.formatters.json_generator import generate_aggregated_json

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
                raw_metadata = process_csv(file_path)
            elif ext == '.parquet':
                raw_metadata = process_parquet(file_path)
            elif ext in ['.json', '.jsonl']:
                raw_metadata = process_json(file_path)
            else:
                logger.info(f"  -> [Modulo para {ext} pendiente de implementacion]")
            
            if raw_metadata:
                if "sample" in raw_metadata and raw_metadata["sample"]:
                    raw_metadata["sample"] = sanitize_sample(raw_metadata["sample"])
                all_metadata.append(raw_metadata)
                
        except Exception as e:
            logger.error(f"Error procesando {file_path.name}: {str(e)}")

    # Generacion del reporte consolidado en la raiz del proyecto
    if all_metadata:
        project_root = Path(__file__).parent
        input_name = target_path.name if target_path.is_dir() else target_path.stem
        
        # 1. Generar y guardar Markdown
        md_filename = f"{input_name}_blueprint.md"
        md_path = project_root / md_filename
        final_report_md = generate_aggregated_markdown(all_metadata, input_name)
        
        with open(md_path, "w", encoding="utf-8") as f:
            f.write(final_report_md)
        logger.info(f"Reporte MD generado en: {md_path}")

        # 2. Generar y guardar JSON
        json_filename = f"{input_name}_blueprint.json"
        json_path = project_root / json_filename
        final_report_json = generate_aggregated_json(all_metadata, input_name)
        
        with open(json_path, "w", encoding="utf-8") as f:
            f.write(final_report_json)
        logger.info(f"Reporte JSON generado en: {json_path}")

if __name__ == "__main__":
    main()