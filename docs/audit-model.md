# Modelo de auditoría

## `pipeline_executions`

Una fila por ejecución con selección, tiempos, estado, conteos y ruta del manifiesto.

## `file_availability`

Una fila por periodo revisado y ejecución. Incluye `NOT_APPLICABLE`, `NOT_PUBLISHED_YET`, `AVAILABLE` y `FAILED_TO_PROBE`. Esta colección alimentará el futuro dashboard de control.

## `file_registry`

Una fila vigente por `service + year + month`. Mantiene estado actual, claim temporal y metadata de la versión publicada.

## `file_versions`

Una fila por `service + year + month + sha256`. Conserva la historia de archivos oficiales, metadata remota, esquema, ruta vigente o archivada y ejecución responsable.

Los viajes individuales no se almacenan en MongoDB.
