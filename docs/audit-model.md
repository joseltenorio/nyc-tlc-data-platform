# Modelo de auditoría y calidad

Los viajes permanecen en Parquet. MongoDB conserva metadata operativa, trazabilidad, calidad y cobertura.

## Contrato unificado para dashboards

### `audit_pipeline_runs`

Una fila documental por ejecución de capa y por ejecución padre `platform`:

```text
execution_id
parent_execution_id
layer
execution_type
status
started_at / finished_at / duration_seconds
selection
metrics
warnings
error_type / error_message
```

### `audit_dataset_events`

Un evento por dataset físico leído, procesado o publicado:

```text
layer, dataset_name, dataset_type, operation, status
path, parquet_files, rows, bytes_on_disk
service, year, month, source_dataset
error_type, error_message, metadata
```

Permite obtener número de Parquet por capa sin contar viajes en MongoDB.

### `audit_quality_events`

Un documento por regla:

```text
rule_code, dimension, severity, status
expected, actual, failed_rows, message, context
```

Dimensiones usadas: validez, completitud, reconciliación, confiabilidad y observabilidad.

### `audit_coverage_snapshots`

Fotografía por ejecución/capa:

```text
expected_count
available_count
ready_count
missing_count
not_applicable_count
not_published_count
deferred_count
coverage_rate
missing
details
```

`NOT_PUBLISHED_YET` no se considera pérdida mientras `treat_not_published_as_missing=false`.

### `audit_download_attempts`

Un documento por intento completo de descarga Bronze. Cinco reintentos producen como máximo seis documentos para un archivo.

## Colecciones específicas

Se conservan las colecciones Bronze, Silver, Gold y ML existentes porque contienen detalles transaccionales adicionales, como claims, versiones, reglas por archivo, métricas de modelos y reconciliaciones.

## Manifiestos

```text
data/manifests/*.json
data/manifests/silver/*.json
data/manifests/gold/*.json
data/manifests/ml/*.json
```

Los manifiestos permiten revisar ejecuciones aunque MongoDB no esté disponible. Streamlit los usa como fallback.
