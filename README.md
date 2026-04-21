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
   '''bash
   git clone https://github.com/ferfections/data-blueprint.git
   cd DataBlueprint
   '''

2. Crea y activa un entorno virtual (Recomendado):
   '''bash
   python -m venv .venv
   source .venv/bin/activate  # En Windows usa: .venv\Scripts\activate
   '''

3. Instala el paquete en modo editable (esto instalara Polars y configurara el comando CLI):
   '''bash
   pip install -e .
   '''

---

## Guia de Uso

Una vez instalado, el comando `data-blueprint` estara disponible globalmente en tu terminal. La herramienta tiene dos modos de operacion principales:

### 1. Generacion de Blueprints (Perfilado)
Para extraer los metadatos de un archivo o de una carpeta entera, pasa la ruta como argumento.

# Analizar un directorio completo (Recomendado)
'''bash
data-blueprint ruta/a/mi_landing_zone/
'''

# Analizar un archivo individual
'''bash
data-blueprint ruta/a/mis_datos/clientes.parquet
'''

**Salida:** El programa generara dos archivos en el directorio donde ejecutaste el comando:
* `[nombre]_blueprint.md`: Resumen tabular listo para lectura humana o para adjuntar a ChatGPT/Claude.
* `[nombre]_blueprint.json`: Diccionario de datos estructurado listo para integraciones M2M.

### 2. Deteccion de Schema Drift (Auditoria)
Para comprobar si la estructura de tus datos ha mutado sin previo aviso, puedes comparar un Blueprint que generaste en el pasado (tu "contrato") con uno generado hoy.

'''bash
data-blueprint --compare contrato_ayer.json datos_hoy.json
'''

**Salida:** El sistema analizara ambos archivos e imprimira por terminal un reporte critico si detecta:
* Archivos desaparecidos.
* Columnas eliminadas o añadidas.
* Mutaciones en los tipos de datos (ej. de `Float64` a `String`).
Si todo esta en orden, confirmara que el contrato se cumple al 100%.

---

## Estructura del Proyecto

DataBlueprint/
├── datablueprint/
│   ├── __init__.py
│   ├── cli.py                    # Interfaz de linea de comandos y orquestador principal
│   ├── core/
│   │   ├── profiler.py           # Logica de extraccion con Polars
│   │   └── drift_detector.py     # Motor de deteccion de Schema Drift
│   ├── formatters/
│   │   ├── json_generator.py     # Generador de reportes JSON
│   │   └── markdown_generator.py # Generador de reportes Markdown
│   └── security/
│       └── pii_masker.py         # Reglas de enmascaramiento de datos sensibles
├── pyproject.toml                # Configuracion de construccion y dependencias de PyPI
├── requirements.txt              # Alternativa para entornos estaticos
├── DataBlueprint_logo.png        # Banner del proyecto
└── README.md


## Proximos Pasos (Roadmap)
* **Fase 2:** Desarrollo de una Interfaz Grafica de Usuario (GUI) interactiva utilizando Streamlit.
* **Code Gen:** Modulo para generar sentencias SQL DDL automaticamente desde el Blueprint JSON.
* **NLP Security:** Integracion de modelos avanzados para deteccion de PII por contexto.