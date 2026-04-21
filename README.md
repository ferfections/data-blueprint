<p align="center">
  <img src="DataBlueprint_logo.png" alt="DataBlueprint Banner" width="100%">
</p>

# DataBlueprint

Motor de perfilado de datos, generacion de contexto seguro para Inteligencia Artificial y deteccion de *Schema Drift*.

## Descripcion

**DataBlueprint** es una herramienta de ingenieria de datos diseñada para actuar como puente seguro entre tus archivos de datos crudos y los Grandes Modelos de Lenguaje (LLMs), asi como para auditar la calidad de tus pipelines de datos.

Enviar datasets completos a una IA consume un exceso de tokens, genera alucinaciones y expone informacion confidencial (PII). DataBlueprint resuelve esto analizando tus directorios locales de forma ultrarrapida y generando un "plano arquitectonico" (esquema, tipos de datos, metricas de calidad y muestras anonimizadas) en formatos legibles tanto para humanos (Markdown) como para maquinas (JSON). 

Ademas, incluye un motor de validacion de contratos de datos para detectar mutaciones silenciosas en los esquemas antes de que rompan tus procesos.

## Caracteristicas Principales

* **Soporte Multiformato:** Lee de forma nativa archivos CSV, Parquet, JSON y JSONL.
* **Rendimiento Extremo:** Motor central construido sobre Polars (Rust), capaz de inferir esquemas en datasets masivos sin colapsar la memoria RAM.
* **Privacidad por Diseño (Data Security):** Modulo de sanitizacion integrado que detecta y enmascara automaticamente informacion sensible (Emails, Telefonos, DNI) mediante expresiones regulares.
* **Deteccion de Schema Drift:** Compara automaticamente "contratos" de datos en formato JSON para alertar sobre columnas eliminadas, nuevas o cambios en los tipos de datos.
* **Procesamiento en Batch:** Capacidad de leer directorios completos y generar un indice global del contexto de los datos.

---

## Instalacion

El proyecto requiere Python 3.9 o superior y funciona mediante un CLI (Command Line Interface).

1. Clona el repositorio:
   ```bash
   git clone https://github.com/ferfections/data-blueprint.git
   cd data-blueprint