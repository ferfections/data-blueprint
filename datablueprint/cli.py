import argparse
import sys
import logging
from pathlib import Path

from datablueprint.core.profiler import process_csv, process_parquet, process_json
from datablueprint.security.pii_masker import sanitize_sample
from datablueprint.formatters.markdown_generator import generate_aggregated_markdown
from datablueprint.formatters.json_generator import generate_aggregated_json
from datablueprint.core.drift_detector import compare_blueprints
from datablueprint.formatters.ddl_generator import generate_sql_ddl

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger("DataBlueprint")

SUPPORTED_EXTENSIONS = {'.csv', '.parquet', '.json', '.jsonl'}

def setup_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="DataBlueprint: Extrae metadatos y detecta Schema Drift.")
    
    # Argumento posicional opcional (nargs='?')
    parser.add_argument("input_path", type=str, nargs="?", help="Ruta a la carpeta o archivo a analizar.")
    
    # Nuevo argumento para comparar
    parser.add_argument(
        "--compare", 
        nargs=2, 
        metavar=('CONTRATO.json', 'NUEVO.json'),
        help="Compara dos archivos JSON para detectar Schema Drift."
    )
    
    return parser

def get_files_to_process(target_path: Path) -> list[Path]:
    if not target_path.exists():
        logger.error(f"La ruta especificada no existe: {target_path}")
        sys.exit(1)

    files_to_process = []
    if target_path.is_file() and target_path.suffix.lower() in SUPPORTED_EXTENSIONS:
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

    # Si el usuario quiere comparar, ejecutamos esa ruta y salimos
    if args.compare:
        compare_blueprints(args.compare[0], args.compare[1])
        sys.exit(0)

    # Si no usa --compare y tampoco pasa una ruta, mostramos la ayuda
    if not args.input_path:
        parser.print_help()
        sys.exit(1)

    # Flujo original de generacion de blueprints
    target_path = Path(args.input_path)
    files = get_files_to_process(target_path)
    
    if not files:
        logger.warning("No se encontraron archivos validos.")
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
            
            if raw_metadata:
                if "sample" in raw_metadata and raw_metadata["sample"]:
                    raw_metadata["sample"] = sanitize_sample(raw_metadata["sample"])
                all_metadata.append(raw_metadata)
                
        except Exception as e:
            logger.error(f"Error procesando {file_path.name}: {str(e)}")

    if all_metadata:
        project_root = Path.cwd() # Usamos Current Working Directory para guardar donde se ejecuta el comando
        input_name = target_path.name if target_path.is_dir() else target_path.stem
        
        md_path = project_root / f"{input_name}_blueprint.md"
        with open(md_path, "w", encoding="utf-8") as f:
            f.write(generate_aggregated_markdown(all_metadata, input_name))
        
        # JSON Generator
        json_path = project_root / f"{input_name}_blueprint.json"
        with open(json_path, "w", encoding="utf-8") as f:
            f.write(generate_aggregated_json(all_metadata, input_name))

        # SQL DDL Generator
        sql_path = project_root / f"{input_name}_blueprint.sql"
        with open(sql_path, "w", encoding="utf-8") as f:
            f.write(generate_sql_ddl(all_metadata))
            
        logger.info(f"Reportes generados en {project_root}")

if __name__ == "__main__":
    main()