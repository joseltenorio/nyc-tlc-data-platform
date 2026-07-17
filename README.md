# NYC TLC Data Platform — Bronze

Implementación completa de la primera etapa del caso **NYC TLC Trip Record Data**. Esta entrega conserva los archivos Parquet oficiales sin transformarlos, registra su trazabilidad y deja el paquete preparado para incorporar posteriormente Silver, Gold, modelos predictivos y dashboards sin volver a reorganizar el repositorio.

## Alcance real de esta entrega

Incluye exclusivamente Bronze y sus componentes necesarios:

- carga histórica predeterminada de **2019 a 2025**;
- carga incremental de los archivos publicados de **2026**;
- Yellow, Green, FHV y High Volume FHV;
- `fhvhv/2019-01` registrado como `NOT_APPLICABLE`;
- descubrimiento desde el HTML oficial y fallback determinista únicamente para periodos faltantes;
- matriz completa `servicio × año × mes`;
- descarga concurrente con ocho workers y máximo dos archivos HVFHV simultáneos;
- archivos temporales `.part`, `fsync`, tamaño, firma Parquet y SHA-256;
- una sola SparkSession para la validación posterior a las descargas;
- metadata física obtenida sin ejecutar `count()` completo;
- detección de evolución de esquema sin modificar el archivo;
- publicación atómica y conservación de versiones oficiales anteriores;
- auditoría normalizada en MongoDB;
- manifiesto JSON por ejecución;
- comandos `plan`, `historical`, `incremental` y `run`.

No contiene transformaciones Silver, hechos o dimensiones Gold, modelos ML ni dashboards. Tampoco crea carpetas vacías para esas fases.

## Estructura

```text
nyc-tlc-data-platform/
├── config/
│   ├── app.yml
│   ├── tlc_sources.yml
│   ├── bronze.yml
│   └── schema_contracts.yml
├── src/tlc_data_platform/
│   ├── core/
│   ├── ingestion/
│   ├── bronze/
│   ├── audit/
│   ├── mongodb/
│   ├── orchestration/
│   └── cli/
├── tests/
│   ├── unit/
│   └── integration/
├── notebooks/
├── docs/
├── data/
├── Dockerfile
├── docker-compose.yml
└── pyproject.toml
```

## Flujo Bronze

```text
Matriz esperada
      ↓
HTML oficial
      ↓
Fallback para periodos faltantes
      ↓
Probe remoto y plan de ejecución
      ↓
Reclamo atómico en MongoDB
      ↓
Descargas concurrentes a .part
      ↓
Validación física + PySpark
      ↓
Publicación atómica en Bronze
      ↓
Registro actual + historial de versiones
      ↓
Manifiesto y resumen de ejecución
```

Los Parquet no se renombran internamente, no se limpian, no se deduplican y no se reescriben. Bronze conserva el original publicado por TLC.

## Preparación

```powershell
Copy-Item .env.example .env

docker compose build
docker compose up -d mongodb
```

## Plan antes de descargar

El plan consulta disponibilidad y muestra cobertura, pendientes, tamaño remoto conocido y espacio libre. No marca archivos como procesados ni crea versiones.

```powershell
docker compose run --rm bronze plan
```

Plan pequeño:

```powershell
docker compose run --rm bronze plan `
  --services yellow `
  --start-year 2026 `
  --end-year 2026 `
  --months 1 2
```

## Carga histórica 2019–2025

```powershell
docker compose run --rm bronze historical
```

## Carga incremental 2026

```powershell
docker compose run --rm bronze incremental
```

El pipeline solo procesa los archivos ya publicados. Los meses futuros quedan como `NOT_PUBLISHED_YET`.

## Rango personalizado

```powershell
docker compose run --rm bronze run `
  --services yellow green `
  --start-year 2025 `
  --end-year 2025 `
  --months 1 2 `
  --workers 4 `
  --max-hvfhv-workers 2
```

## Dry run y reprocesamiento

```powershell
docker compose run --rm bronze incremental --dry-run
```

```powershell
docker compose run --rm bronze run `
  --services yellow `
  --start-year 2026 `
  --end-year 2026 `
  --months 1 `
  --force
```

Los argumentos de CLI sobrescriben la configuración en memoria; nunca editan los YAML.

## Salidas

Archivo vigente:

```text
data/bronze/trip_records/<service>/year=YYYY/month=MM/<service>_tripdata_YYYY-MM.parquet
```

Versiones sustituidas:

```text
data/bronze/versions/trip_records/<service>/year=YYYY/month=MM/
```

Manifiestos:

```text
data/manifests/<execution_id>.json
```

Colecciones MongoDB:

```text
pipeline_executions
file_availability
file_registry
file_versions
```

## Estados

Cobertura y disponibilidad:

```text
NOT_APPLICABLE
NOT_PUBLISHED_YET
AVAILABLE
FAILED_TO_PROBE
DISCOVERED
```

Procesamiento:

```text
PENDING
DOWNLOADING
DOWNLOADED
VALIDATING
READY
SKIPPED_UNCHANGED
SKIPPED_CLAIMED
FAILED
```

Ejecución:

```text
RUNNING
SUCCESS
PARTIAL_SUCCESS
FAILED
DRY_RUN
```

## Validación Parquet

Se comprueba:

- firma inicial y final `PAR1`;
- archivo no vacío y legible;
- consistencia del nombre con servicio, año y mes;
- campos requeridos y tipos esperados;
- columnas opcionales ausentes;
- columnas nuevas;
- hash del esquema;
- `num_rows`, `num_row_groups`, `num_columns`, `created_by` y codecs;
- lectura de una fila de muestra con PySpark, sin un `count()` completo.

Una columna nueva genera `schema_evolution_detected=true`, pero no invalida el archivo. Una columna requerida ausente sí produce `FAILED`.

## Pruebas

```powershell
docker compose run --rm bronze pytest
```

Las pruebas usan mocks y Parquet pequeños; no descargan el histórico real.

## Solución de problemas

**MongoDB no está disponible:** ejecute `docker compose up -d mongodb` y revise `.env`.

**Java no encontrado:** use la imagen Docker incluida. PySpark requiere Java y la imagen instala OpenJDK 17.

**Espacio insuficiente:** ejecute `plan`, libere espacio o cambie las rutas de `config/bronze.yml`.

**HTTP 429 o 5xx:** el cliente respeta `Retry-After` y aplica reintentos limitados con backoff.

**Mes 2026 no publicado:** no es un fallo; se registra como `NOT_PUBLISHED_YET`.

## Fuentes

- Página oficial TLC: `https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page`
- Patrón Parquet: `https://d37ci6vzurychx.cloudfront.net/trip-data/<service>_tripdata_YYYY-MM.parquet`
