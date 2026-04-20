# DataBlueprint

Motor de perfilado de datos y generacion de contexto seguro para Inteligencia Artificial.

## Descripcion

DataBlueprint es una herramienta de ingenieria de datos diseñada para actuar como puente entre tus archivos de datos crudos y los Grandes Modelos de Lenguaje (LLMs). 

Enviar datasets completos a una IA consume un exceso de tokens, genera alucinaciones y, lo mas critico, expone informacion confidencial y datos personales (PII). DataBlueprint resuelve esto analizando tus directorios locales de forma ultrarrapida y generando un "plano arquitectonico" (esquema, tipos de datos, metricas de calidad y muestras anonimizadas) en formatos legibles tanto para humanos como para maquinas.

## Caracteristicas Principales

* **Soporte Multiformato:** Lee de forma nativa archivos CSV, Parquet, JSON y JSONL.
* **Rendimiento Extremo:** Motor central construido sobre Polars (Rust), capaz de inferir esquemas en datasets masivos sin colapsar la memoria RAM.
* **Privacidad por Diseño (Data Security):** Modulo de sanitizacion integrado que detecta y enmascara automaticamente informacion sensible (Emails, Telefonos, DNI) mediante expresiones regulares antes de la exportacion.
* **Doble Salida Optimizada:** Genera un reporte consolidado en Markdown (ideal para adjuntar en prompts a ChatGPT o Claude) y en JSON (ideal para integraciones M2M y pipelines automatizados).
* **Procesamiento en Lote (Batch):** Capacidad de leer directorios completos y generar un indice global del contexto de los datos.

## Estructura del Proyecto

```text
DataBlueprint/
├── datablueprint/
│   ├── core/
│   │   └── profiler.py           # Logica de extraccion con Polars
│   ├── formatters/
│   │   ├── json_generator.py     # Generador de reportes JSON
│   │   └── markdown_generator.py # Generador de reportes Markdown
│   └── security/
│       └── pii_masker.py         # Reglas de enmascaramiento de datos sensibles
├── main.py                       # Orquestador y CLI
├── requirements.txt              # Dependencias del proyecto
└── README.md