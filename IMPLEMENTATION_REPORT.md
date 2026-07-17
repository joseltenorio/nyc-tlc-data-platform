# Informe de implementación — Bronze + Silver

## Resultado

El repositorio `nyc-tlc-data-platform` conserva la implementación Bronze existente y añade una capa Silver completa para Yellow, Green, FHV y HVFHV. La nueva capa procesa únicamente archivos Bronze vigentes con estado `READY`, separa registros aceptados y rechazados, enriquece zonas y bases, genera un contrato transversal `trips_master` y registra calidad y reconciliación en MongoDB.

No se añadieron todavía tablas Gold, modelos predictivos ni dashboards.

## Principios conservados de Bronze

No se reemplazó la lógica ya implementada de:

- descubrimiento HTML y fallback determinista;
- matriz de periodos 2019–2026;
- tratamiento de `fhvhv/2019-01` como `NOT_APPLICABLE`;
- descarga a `.part`, checksum y validación Parquet;
- publicación atómica e historial de versiones;
- claims recuperables;
- auditoría y manifiestos Bronze;
- comandos `plan`, `historical`, `incremental` y `run`.

Los 15 tests de integración Bronze existentes continúan aprobando.

## Archivos principales creados

### Configuración

```text
config/silver.yml
```

### Código Silver

```text
src/tlc_data_platform/silver/
├── __init__.py
├── audit.py
├── enrichment.py
├── manifest.py
├── master.py
├── models.py
├── pipeline.py
├── references.py
├── source_catalog.py
├── spark.py
├── storage.py
└── transformers/
    ├── __init__.py
    ├── common.py
    ├── taxi.py
    ├── yellow.py
    ├── green.py
    ├── fhv.py
    └── fhvhv.py
```

### Auditoría e índices

```text
src/tlc_data_platform/audit/silver_execution_repository.py
src/tlc_data_platform/audit/silver_file_registry_repository.py
src/tlc_data_platform/audit/silver_quality_repository.py
src/tlc_data_platform/audit/silver_reconciliation_repository.py
src/tlc_data_platform/mongodb/silver_index_manager.py
```

### Orquestación

```text
src/tlc_data_platform/orchestration/silver_pipeline.py
src/tlc_data_platform/orchestration/medallion_pipeline.py
```

### Pruebas

```text
tests/unit/test_silver_manifest.py
tests/unit/test_silver_registry.py
tests/unit/test_silver_settings.py
tests/unit/test_silver_source_catalog.py
tests/unit/test_silver_storage.py
tests/integration/test_silver_transformers.py
tests/integration/test_silver_references.py
tests/integration/test_silver_pipeline.py
```

### Documentación y notebooks

```text
docs/silver-architecture.md
docs/silver-data-quality.md
docs/silver-data-dictionary.md
docs/silver-execution-guide.md
notebooks/04_silver_plan.ipynb
notebooks/05_execute_historical_silver.ipynb
notebooks/06_execute_incremental_silver.ipynb
notebooks/07_refresh_silver_references.ipynb
```

## Archivos principales modificados

```text
README.md
IMPLEMENTATION_REPORT.md
pyproject.toml
Dockerfile
docker-compose.yml
.gitignore
.dockerignore
config/app.yml
src/tlc_data_platform/core/settings.py
src/tlc_data_platform/core/exceptions.py
src/tlc_data_platform/cli/main.py
src/tlc_data_platform/audit/summaries.py
docs/audit-model.md
docs/bronze-architecture.md
docs/execution-guide.md
notebooks/00_environment_validation.ipynb
tests/conftest.py
tests/unit/test_cli.py
```

No se eliminaron módulos funcionales de Bronze.

## Funcionalidad Silver implementada

### Selección de fuentes

- Consulta `file_registry` Bronze.
- Exige `status=READY` por defecto.
- Verifica existencia física del Parquet.
- Conserva SHA-256, ejecución Bronze y número de filas de metadata.
- Distingue `BRONZE_READY`, `BRONZE_NOT_READY` y `NOT_APPLICABLE` en el plan.

### Transformaciones por servicio

Yellow y Green:

- tipado y homologación de vendor, tarifa, pago y banderas;
- timestamps locales `timestamp_ntz`;
- pasajeros, distancia y componentes financieros;
- duración, velocidad, tarifa por milla, porcentaje de propina e ingreso por minuto;
- campos específicos Green (`trip_type`, `ehail_fee`).

FHV:

- bases despachadora y afiliada;
- fechas y zonas homologadas;
- normalización de `SR_Flag`;
- duración y viaje aeroportuario.

HVFHV:

- solicitud, llegada, pickup y dropoff;
- millas, tiempo informado y componentes financieros;
- shared ride, Access-A-Ride y WAV;
- espera del conductor y solicitud–pickup;
- mapeo de licenciatarios HVFHS conocido y advertencia ante códigos nuevos.

### Calidad

- reglas `ERROR` y `WARNING` por fila;
- arrays de códigos de calidad;
- deduplicación determinista por SHA-256;
- registros rechazados preservados físicamente;
- registros con advertencias conservados en Silver curado;
- conteos de reglas en `silver_quality_results`.

### Referencias

- descarga de Taxi Zone Lookup y Current Bases;
- validación de CSV y cabeceras;
- copia raw inmutable en Bronze por SHA-256;
- normalización a Parquet Silver;
- joins broadcast de pickup/dropoff y bases;
- faltantes de zona como error;
- bases históricas ausentes del catálogo vigente como advertencia.

### Salidas

```text
data/silver/yellow_trips/
data/silver/green_trips/
data/silver/fhv_trips/
data/silver/hvfhv_trips/
data/silver/rejected_records/
data/silver/trips_master/
data/silver/taxi_zones/
data/silver/base_lookup/
```

### Reconciliación

Por archivo se comprueba:

```text
rows_read = rows_valid + rows_rejected
```

Cuando Bronze contiene `parquet_num_rows`, también se compara contra la lectura Spark.

### Idempotencia y publicación

- claim atómico por servicio, año y mes;
- recuperación de claims vencidos, huérfanos o de ejecuciones finalizadas;
- omisión por SHA Bronze sin cambios y salidas presentes;
- escritura en temporales;
- promoción recuperable de curated, rejected y master;
- `--force` para reprocesamiento explícito.

## Comandos añadidos

```text
silver-plan
silver-historical
silver-incremental
silver-run
silver-references
medallion-historical
medallion-incremental
medallion-run
```

## Validaciones ejecutadas

### Compilación y configuración

```text
python -m compileall -q src tests
Carga válida de 5 archivos YAML
Validación JSON de 8 notebooks
python -m tlc_data_platform --help
```

### Pruebas

Se recolectaron **94 pruebas**:

```text
72 unitarias
15 integración Bronze
7 integración Silver con Spark
```

Resultados ejecutados:

```text
72/72 pruebas unitarias aprobadas
15/15 pruebas de integración Bronze aprobadas
5/5 pruebas de transformadores Silver aprobadas
1/1 prueba de referencias Silver aprobada
1/1 prueba end-to-end Silver aprobada
```

Total validado por grupos:

```text
94 aprobadas
0 fallidas
```

La prueba end-to-end Silver creó Parquet pequeños, escribió curated/rejected/master y verificó la reconciliación de filas.

## Limitaciones de validación del entorno

- No se descargó el histórico real 2019–2025 ni los meses reales de 2026.
- No se consultaron referencias oficiales durante los tests; se utilizaron respuestas controladas.
- No se levantó MongoDB real; repositorios, claims e índices se validaron con dobles.
- No se construyó la imagen Docker porque el entorno no dispone del comando/daemon Docker.
- El intérprete disponible para la validación fue Python 3.13, fuera del rango soportado del proyecto. Por estabilidad de workers PySpark, los módulos Spark se ejecutaron en procesos separados. El proyecto exige Python 3.11 o 3.12 y la imagen Docker usa Python 3.12.
- No se realizaron commits, pushes ni ramas.

## Comandos de ejecución

Preparación:

```powershell
Copy-Item .env.example .env
docker compose build
docker compose up -d mongodb
```

Bronze + Silver histórico:

```powershell
docker compose run --rm pipeline medallion-historical
```

Bronze + Silver incremental:

```powershell
docker compose run --rm pipeline medallion-incremental
```

Solo Silver:

```powershell
docker compose run --rm silver silver-plan
docker compose run --rm silver silver-references
docker compose run --rm silver silver-historical
```

Pruebas:

```powershell
docker compose run --rm pipeline pytest
```
