# Modelo de auditoría y calidad

Los viajes permanecen en Parquet. La auditoría conserva únicamente hechos operativos,
metadata, trazabilidad, calidad, cobertura y métricas físicas; no copia viajes individuales ni
crea valores sintéticos para completar dashboards.

## Principio de persistencia dual

Cada evento del contrato unificado se escribe con el mismo identificador en:

1. MongoDB, como repositorio operacional y consultable.
2. JSONL append-only, como evidencia durable e independiente para el dashboard y análisis
   post-mortem.

El dashboard combina MongoDB, JSONL y manifests antiguos, deduplica por identificadores de
negocio y usa la evidencia con la marca temporal más reciente, completándola con campos de las
otras fuentes. La ausencia de un dato se representa como
vacío, nunca como cero o valor estimado, salvo que el cero haya sido registrado realmente.

## Contrato JSONL por capa

```text
data/audit/
├── bronze/
│   ├── pipeline_runs.jsonl
│   ├── dataset_events.jsonl
│   ├── quality_events.jsonl
│   ├── coverage_snapshots.jsonl
│   └── download_attempts.jsonl
├── silver/
│   ├── pipeline_runs.jsonl
│   ├── dataset_events.jsonl
│   ├── quality_events.jsonl
│   └── coverage_snapshots.jsonl
├── gold/
│   ├── pipeline_runs.jsonl
│   ├── dataset_events.jsonl
│   ├── quality_events.jsonl
│   └── coverage_snapshots.jsonl
├── ml/
│   ├── pipeline_runs.jsonl
│   ├── dataset_events.jsonl
│   ├── quality_events.jsonl
│   └── coverage_snapshots.jsonl
└── inventory/
    ├── inventory_snapshots.jsonl
    └── medallion_inventory.json
```

Cada línea JSONL es un objeto completo con `audit_schema_version`, `event_type`, `layer` y
`written_at`. Los archivos se abren en modo append para no reescribir evidencia histórica.

### `pipeline_runs.jsonl`

Una corrida genera eventos `START`, `LINK_PARENT`, `FINISH` o `FAIL`. El lector consolida esos
eventos mediante `execution_id`.

```text
execution_id, parent_execution_id, layer, execution_type
status, started_at, finished_at, duration_seconds
selection, metrics, warnings
error_type, error_message, event_action
```

### `dataset_events.jsonl`

Un evento por dataset físico leído, procesado, omitido o publicado:

```text
event_id, execution_id, layer
dataset_name, dataset_type, operation, status
path, parquet_files, rows, bytes_on_disk
service, year, month, source_dataset
error_type, error_message, metadata, recorded_at
```

`parquet_files`, `rows` y `bytes_on_disk` solo aparecen cuando la capa pudo medirlos.

### `quality_events.jsonl`

Un evento por regla evaluada:

```text
quality_id, execution_id, layer, dataset_name
rule_code, dimension, severity, status
expected, actual, failed_rows, message, context, checked_at
```

Las dimensiones incluyen validez, completitud, reconciliación, confiabilidad y observabilidad.

### `coverage_snapshots.jsonl`

Fotografía de cobertura por ejecución y capa:

```text
expected_count, available_count, ready_count, missing_count
not_applicable_count, not_published_count, deferred_count
coverage_rate, missing, details, checked_at
```

`NOT_PUBLISHED_YET` no se considera pérdida mientras
`treat_not_published_as_missing=false`. Cuando no existe ningún periodo aplicable, el snapshot
usa `status=NO_SCOPE` y `coverage_rate=null`; nunca se fabrica una cobertura de 100 %.

### `download_attempts.jsonl`

Un evento por intento HTTP completo de Bronze:

```text
service, year, month, url
attempt_number, retry_number, max_attempts, outcome, status_code
started_at, finished_at, duration_seconds
bytes_downloaded, expected_bytes, throughput_bytes_per_second
retry_delay_seconds, error_type, error_message
```

El porcentaje de error se calcula con el resultado final real de cada archivo. Los reintentos
intermedios no se cuentan como archivos fallidos.

## Inventario físico Medallion

Al finalizar o fallar una corrida se escanean las raíces configuradas de Bronze, Silver, Gold y
ML. Bronze usa `data/bronze` completo, por lo que el conteo físico incluye cualquier Parquet
de viajes activos, versiones históricas o áreas de referencia; los CSV crudos de referencia no se
contabilizan como Parquet. El detalle por dataset permite distinguir los archivos medidos. El snapshot
registra:

```text
layer, root, root_exists
parquet_files, bytes_on_disk, dataset_count
latest_modified_at, scan_error_count, scan_errors
datasets[]: dataset_name, parquet_files, bytes_on_disk
```

`medallion_inventory.json` contiene el snapshot actual para KPIs del dashboard.
`inventory_snapshots.jsonl` conserva su historia. Por tanto, el número de archivos por capa no
se obtiene sumando eventos históricos, lo que evitaría duplicaciones. Si todavía no existe un
snapshot, el dashboard muestra ausencia de evidencia (`—`) y no un cero inventado.

## Colecciones MongoDB equivalentes

```text
audit_pipeline_runs
audit_dataset_events
audit_quality_events
audit_coverage_snapshots
audit_download_attempts
```

Las colecciones específicas de Bronze, Silver, Gold y ML se conservan porque contienen claims,
versiones, reglas por archivo, métricas de modelos y reconciliaciones adicionales.

## Manifests por capa

```text
data/manifests/bronze/<execution_id>.json
data/manifests/silver/<execution_id>.json
data/manifests/silver/references/<execution_id>.json
data/manifests/gold/<execution_id>.json
data/manifests/ml/<execution_id>.json
```

Todos incluyen `manifest_schema_version`, `layer`, `manifest_type` y `summary`. Los refrescos de
referencias se mantienen dentro de la capa Silver, pero en una subcarpeta propia para no mezclarlos
con las ejecuciones de transformación. El lector conserva compatibilidad con manifests antiguos
ubicados directamente en `data/manifests/`, pero las nuevas ejecuciones ya no los escriben allí.
