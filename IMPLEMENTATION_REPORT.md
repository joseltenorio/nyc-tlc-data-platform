# Informe de corrección: seguridad, compatibilidad, auditoría y calidad

## Objetivo

Corregir la plataforma completa para que el flujo Bronze → Silver → Gold → ML → Streamlit sea coherente con el alcance 2023–2025 + 2026, no vuelva a generar crecimiento descontrolado dentro del disco virtual de Docker y exponga información operativa útil para los dashboards.

## Alcance de datos aplicado

- Yellow, Green y FHV: enero de 2023 a diciembre de 2026; en 2026 solo se procesan archivos ya publicados.
- HVFHV: enero a diciembre de 2023.
- Cualquier periodo fuera de esas ventanas se registra como `NOT_APPLICABLE`.
- El alcance se aplica en Bronze, Silver y Gold; no depende únicamente de filtros visuales del dashboard.

La carga histórica predeterminada contiene 120 particiones aplicables:

```text
Yellow  36
Green   36
FHV     36
HVFHV   12
Total  120
```

La selección incremental 2026 contiene 36 periodos aplicables antes de descontar meses todavía no publicados.

## Corrección del crecimiento de Docker/WSL

La causa principal era la combinación de DataFrames Gold completos persistidos, conteos repetidos, reconstrucción global y spill almacenado dentro del filesystem del contenedor.

Se implementó:

- procesamiento Gold secuencial por partición Silver;
- eliminación de cachés globales de hechos;
- lectura con poda de columnas para proveedores;
- lectura analítica limitada a las particiones configuradas;
- `local[2]`, driver de 3 GB y 512 particiones para Silver/Gold/ML;
- contenedor de 5 GB y 2 CPU;
- directorios Spark dedicados bajo `data/tmp/spark/<layer>/run-*`;
- monitor de espacio temporal y espacio libre;
- cancelación de jobs al superar límites;
- limpieza en cierre normal y en error;
- detención del dashboard durante las fases Spark;
- validación obligatoria de `.wslconfig` antes del comando completo.

## Publicación y recuperación

Silver, Gold y ML verifican `_SUCCESS` y archivos Parquet antes de publicar.

Gold y ML usan el patrón:

```text
staging → validar → renombrar salida previa → promover → eliminar respaldo
```

Si la promoción falla, se restaura la salida anterior. También se recuperan respaldos `.previous-*` abandonados por interrupciones anteriores.

## Descargas Bronze

`max_retries: 5` significa:

```text
1 intento inicial + hasta 5 reintentos = 6 intentos totales
```

Se reintenta la transferencia completa para HTTP 202, 403, 429, 5xx y fallos de timeout/conexión/streaming. Cada intento elimina el `.part` anterior y reinicia la transferencia para evitar archivos concatenados o truncados.

Cada intento registra:

- servicio, año y mes;
- URL;
- número de intento y máximo;
- código HTTP;
- `SUCCESS`, `RETRY` o `EXHAUSTED`;
- demora antes del siguiente intento;
- tipo y mensaje de error.

## Auditoría unificada

Se añadieron colecciones estables consumidas por Streamlit:

| Colección | Contenido |
|---|---|
| `audit_pipeline_runs` | corridas Bronze, Silver, Gold, ML y padre `platform` |
| `audit_dataset_events` | Parquet de entrada/salida, filas, bytes, ruta y estado |
| `audit_quality_events` | reglas de validez, completitud, reconciliación y confiabilidad |
| `audit_coverage_snapshots` | periodos/datasets esperados, listos y ausentes |
| `audit_download_attempts` | intentos y reintentos de descarga Bronze |

Las corridas hijas se enlazan mediante `parent_execution_id` a la corrida de plataforma.

## Calidad por capa

### Bronze

- contrato de esquema y tipos;
- archivo no vacío;
- firma y metadata Parquet;
- presencia física del archivo publicado;
- análisis de todos los periodos esperados;
- distinción entre ausente, no publicado, diferido y no aplicable.

### Silver

- reglas `ERROR` y `WARNING` por servicio;
- deduplicación determinista;
- rechazados preservados;
- reconciliación `read = valid + rejected`;
- comparación con filas Bronze cuando hay metadata;
- validación de salidas idempotentes;
- cobertura de todas las particiones aplicables.

### Gold

- reconciliación Silver `trips_master` → `fact_trip_activity` por periodo;
- presencia física de dimensiones, hechos, marts y features;
- métricas Parquet después de publicar;
- cobertura de outputs esperados;
- exclusión de periodos fuera del alcance.

### ML

- features de entrada registradas;
- splits temporalmente no vacíos;
- métricas registradas;
- outputs físicos por modelo;
- cobertura de modelos solicitados;
- continuidad con `PARTIAL_SUCCESS` cuando un modelo aislado falla.

## Dashboard de auditoría

La página `10_Auditoria_Pipeline.py` presenta:

- KPIs de corridas, tasa operativa, Parquet, reintentos, reglas fallidas y ausencias;
- Parquet y bytes por capa/tipo/estado;
- cobertura esperada;
- calidad y reconciliaciones;
- errores e intentos HTTP;
- historial de corridas.

Usa MongoDB y combina manifiestos JSON como fallback.

## Orquestación completa

Comando principal:

```powershell
powershell -ExecutionPolicy Bypass -File .\scripts\run-all.ps1
```

El dashboard se inicia al terminar y también después de un fallo de una capa, permitiendo revisar la auditoría. El comando conserva un código de error cuando la plataforma falló.

## Validaciones realizadas

- carga de todos los YAML;
- compilación de `src`, `dashboard` y `tests`;
- análisis estático Ruff sin hallazgos;
- suite unitaria/integración ligera completa aprobada;
- pruebas específicas del alcance 120/36;
- pruebas de cinco reintentos;
- pruebas de recuperación/publicación Silver;
- pruebas de rutas de partición Gold compatibles con Spark.

No se ejecutó una carga real de cientos de GB dentro del entorno de generación de esta entrega. La validación end-to-end con tus datos debe realizarse localmente y queda protegida por los límites de memoria, disco y staging descritos.
