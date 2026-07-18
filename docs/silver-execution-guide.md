# Guía de ejecución Silver

## Requisitos

```text
Python 3.11 o 3.12
Java 17
MongoDB 8
PySpark 4.x
```

La vía recomendada es Docker Compose para mantener versiones reproducibles.

## Preparación

```powershell
Copy-Item .env.example .env
docker compose build
docker compose up -d mongodb
```

## 1. Confirmar Bronze

Silver procesa solamente archivos registrados como `READY` por Bronze. Antes de Silver:

```powershell
docker compose run --rm bronze plan
```

Carga histórica:

```powershell
docker compose run --rm bronze historical
```

Carga incremental:

```powershell
docker compose run --rm bronze incremental
```

## 2. Plan Silver

```powershell
docker compose run --rm silver silver-plan
```

Rango pequeño:

```powershell
docker compose run --rm silver silver-plan `
  --services yellow `
  --start-year 2025 `
  --end-year 2025 `
  --months 1
```

El plan no transforma datos. Informa periodos Bronze READY, ausentes, ya procesados y pendientes.

## 3. Referencias

Actualizar explícitamente:

```powershell
docker compose run --rm silver silver-references
```

Si las referencias faltan y `refresh_references_if_missing=true`, el pipeline las obtiene antes de procesar el primer archivo.

## 4. Ejecutar Silver

Histórico configurado (Yellow/Green/FHV 2023–2025 y HVFHV 2023):

```powershell
docker compose run --rm silver silver-historical
```

Incremental 2026:

```powershell
docker compose run --rm silver silver-incremental
```

Rango personalizado:

```powershell
docker compose run --rm silver silver-run `
  --services yellow green fhv fhvhv `
  --start-year 2025 `
  --end-year 2025 `
  --months 1 2
```

Forzar reprocesamiento:

```powershell
docker compose run --rm silver silver-run `
  --services yellow `
  --start-year 2025 `
  --end-year 2025 `
  --months 1 `
  --force
```

Actualizar referencias en la misma ejecución:

```powershell
docker compose run --rm silver silver-incremental --refresh-references
```

## 5. Ejecutar Bronze y Silver juntos

Histórico:

```powershell
docker compose run --rm pipeline medallion-historical
```

Incremental:

```powershell
docker compose run --rm pipeline medallion-incremental
```

Rango explícito:

```powershell
docker compose run --rm pipeline medallion-run `
  --services yellow green `
  --start-year 2025 `
  --end-year 2025 `
  --months 1 2 `
  --workers 1
```

La secuencia combinada no envía a Silver periodos que Bronze no dejó en `READY`.

## Estados Silver

Por archivo:

```text
PROCESSING
READY
SKIPPED_UNCHANGED
SKIPPED_CLAIMED
FAILED
```

Por ejecución:

```text
SUCCESS
PARTIAL_SUCCESS
FAILED
NO_INPUT
```

## Reanudación

Una nueva ejecución:

- omite salidas vigentes cuando el SHA Bronze no cambió;
- recupera claims vencidos, huérfanos o pertenecientes a ejecuciones finalizadas;
- conserva los periodos ya publicados correctamente;
- puede reprocesar selectivamente con `--force`.

## Pruebas

```powershell
docker compose run --rm pipeline pytest
```

Las pruebas trabajan con Parquet pequeños y no descargan el histórico real.
