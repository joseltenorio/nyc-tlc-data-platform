# Modelo de auditoría Bronze y Silver

MongoDB almacena control operativo y trazabilidad. Los viajes permanecen en Parquet.

## Bronze

### `pipeline_executions`

Una fila documental por ejecución Bronze. Incluye selección, tiempos, estado, conteos y manifiesto.

### `file_availability`

Una fila por periodo revisado y ejecución. Estados principales:

```text
NOT_APPLICABLE
NOT_PUBLISHED_YET
AVAILABLE
FAILED_TO_PROBE
DEFERRED_REMOTE_ACCESS
```

### `file_registry`

Una fila vigente por:

```text
service + year + month
```

Mantiene estado actual, claim, checksum, ruta física, metadata remota y validación Parquet.

### `file_versions`

Una fila por:

```text
service + year + month + sha256
```

Conserva versiones oficiales vigentes o archivadas.

## Silver

### `silver_pipeline_executions`

Una fila por ejecución Silver con:

- selección solicitada;
- estado;
- cantidad de fuentes, procesadas, omitidas y fallidas;
- filas leídas, válidas, rechazadas y con advertencias;
- resultado de actualización/reutilización de referencias;
- ruta del manifiesto.

### `silver_file_registry`

Una fila vigente por:

```text
service + year + month
```

Contiene:

- checksum Bronze utilizado;
- estado Silver;
- paths curado, rechazado y master;
- conteos de filas;
- reconciliación;
- claim temporal;
- última ejecución.

### `silver_quality_results`

Una fila por:

```text
execution_id + service + year + month + rule_code
```

Incluye severidad y cantidad de filas afectadas.

### `silver_reconciliations`

Una fila por archivo procesado. Registra:

```text
bronze_num_rows
rows_read
rows_valid
rows_rejected
reconciliation_status
```

## Claims

Bronze y Silver usan claims con expiración para evitar procesamiento concurrente del mismo periodo. El claim puede recuperarse cuando:

- expiró;
- la ejecución dueña terminó;
- la ejecución dueña no existe;
- pertenece a la misma ejecución.

Los estados finales limpian el claim.

## Manifiestos

Bronze:

```text
data/manifests/<execution_id>.json
```

Silver y referencias:

```text
data/manifests/silver/<execution_id>.json
```

Los manifiestos permiten revisar una ejecución sin consultar MongoDB y complementan, no sustituyen, el registro transaccional.
