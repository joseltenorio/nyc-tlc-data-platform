# NYC TLC Data Platform

Plataforma local end-to-end para los registros de viajes de NYC TLC, implementada con **Python, PySpark, MongoDB, Docker Compose y Streamlit**.

El flujo ejecutable es:

```text
NYC TLC → Bronze → Silver → Gold → ML → Streamlit
                    └──────── auditoría, calidad, cobertura y manifiestos ────────┘
```

## Alcance de datos configurado

La selección del proyecto está limitada explícitamente para evitar descargas accidentales de años o servicios no requeridos:

| Servicio | Cobertura configurada |
|---|---:|
| Yellow | 2023–2025 + meses publicados de 2026 |
| Green | 2023–2025 + meses publicados de 2026 |
| FHV | 2023–2025 + meses publicados de 2026 |
| HVFHV | 2023 únicamente |

Los periodos fuera de ese alcance quedan como `NOT_APPLICABLE`; no se descargan, no se transforman y no se contabilizan como pérdida de datos. Los meses futuros de 2026 quedan como `NOT_PUBLISHED_YET`, nunca como cero.

## Correcciones de seguridad

La ejecución local está deliberadamente priorizada por estabilidad, no por velocidad:

- WSL obligatorio con límite configurable; valores recomendados: 8 GB de RAM, 4 CPU y 2 GB de swap.
- Contenedor de procesamiento limitado a 5 GB y 2 CPU.
- Spark usa `local[2]`, driver de 2–3 GB y 512 particiones de shuffle.
- Bronze procesa una descarga a la vez y HVFHV nunca se paraleliza.
- Silver procesa un archivo mensual por vez.
- Gold procesa una partición mensual por vez y no mantiene simultáneamente hechos completos en caché.
- Gold y ML publican primero en staging y sustituyen la salida anterior mediante promoción recuperable.
- El spill de Spark se escribe en `data/tmp/spark`, visible en Windows, no oculto dentro de `docker_data.vhdx`.
- Cada acción Spark se cancela cuando supera su límite temporal o amenaza la reserva mínima de disco.
- El dashboard se detiene mientras corre Spark y vuelve a iniciarse incluso cuando una capa falla.

## Primera preparación de WSL

Cierra Docker Desktop y ejecuta una sola vez:

```powershell
powershell -ExecutionPolicy Bypass -File .\scripts\configure-wsl.ps1
```

Después abre Docker Desktop y espera a que el motor esté listo.

## Comando único

Desde la raíz del proyecto:

```powershell
powershell -ExecutionPolicy Bypass -File .\scripts\run-all.ps1
```

Este comando:

1. valida límites WSL, Docker y espacio libre;
2. construye la imagen;
3. inicia MongoDB;
4. ejecuta el histórico configurado;
5. ejecuta los meses disponibles de 2026;
6. limpia los temporales Spark;
7. inicia Streamlit en `http://localhost:8501`.

Modos disponibles:

```powershell
# Solo histórico y ML
.\scripts\run-all.ps1 -Mode historical

# Solo actualización 2026; no reentrena ML
.\scripts\run-all.ps1 -Mode incremental

# Todo, pero sin entrenamiento ML
.\scripts\run-all.ps1 -Mode all -SkipML
```

El modo histórico/all exige por defecto 250 GB libres antes de empezar. El incremental exige 80 GB. El parámetro `-MinimumFreeGB` solo debería aumentarse.

## Bronze

Bronze conserva el Parquet oficial sin transformar y registra:

- matriz completa de periodos esperados;
- disponibilidad remota;
- descarga a `.part` y publicación atómica;
- firma Parquet, SHA-256, filas y metadata física;
- hasta **cinco reintentos** por descarga, equivalentes a seis intentos totales;
- estado, código HTTP, demora y error de cada intento;
- versiones sustituidas e idempotencia por checksum.

Comandos directos:

```powershell
docker compose run --rm bronze plan
docker compose run --rm bronze historical
docker compose run --rm bronze incremental
```

## Silver

Silver consume únicamente Bronze `READY`, tipa y homologa columnas, enriquece zonas/bases, separa válidos y rechazados y publica `trips_master`.

Por archivo comprueba:

```text
rows_read = rows_valid + rows_rejected
```

Las salidas mensuales se escriben en temporales y se promueven coordinadamente. Una salida solo se considera reutilizable cuando contiene Parquet y marcador `_SUCCESS`.

## Gold

Gold publica:

- ocho dimensiones conformadas;
- `fact_trip_activity`;
- `fact_taxi_financial`;
- `fact_hvfhv_operations`;
- marts ejecutivos, geográficos, temporales, financieros y operacionales;
- features para forecast, segmentación y wait-risk.

Los hechos se escriben por `service_type/source_year/source_month`. Los marts y features leen solamente las particiones incluidas en el alcance del proyecto, por lo que archivos antiguos que pudieran quedar en disco no contaminan los dashboards.

## Machine Learning

Modelos disponibles:

- pronóstico de demanda;
- segmentación de zonas;
- clasificación de riesgo de espera HVFHV.

El modelo HVFHV usa cortes temporales dentro de 2023. Si un modelo no tiene datos suficientes, la corrida queda `PARTIAL_SUCCESS`, los demás modelos se conservan y el dashboard sigue disponible.

## Auditoría unificada

Cada hecho emitido por el contrato unificado de auditoría se conserva en dos destinos con los mismos identificadores:

1. MongoDB, para consulta operacional.
2. JSONL append-only, para evidencia independiente y lectura del dashboard aunque MongoDB no responda.

```text
data/audit/<layer>/pipeline_runs.jsonl
data/audit/<layer>/dataset_events.jsonl
data/audit/<layer>/quality_events.jsonl
data/audit/<layer>/coverage_snapshots.jsonl
data/audit/bronze/download_attempts.jsonl
data/audit/inventory/inventory_snapshots.jsonl
data/audit/inventory/medallion_inventory.json
```

El inventario se obtiene escaneando físicamente los Parquet de Bronze, Silver, Gold y ML. El
dashboard no genera filas, tiempos, porcentajes ni conteos simulados. Muestra:

- corridas por capa, estado, duración y relación padre/hijo;
- número actual de Parquet, datasets y bytes por capa;
- eventos físicos leídos, publicados, omitidos o fallidos;
- periodos esperados, listos, no publicados, no aplicables y ausentes;
- reglas de calidad, reconciliaciones y filas afectadas;
- tiempo real de descarga, bytes transferidos, velocidad efectiva, reintentos y error final;
- errores de corrida, dataset, regla e intento HTTP.

Los manifests de compatibilidad se escriben por capa en
`data/manifests/{bronze,silver,gold,ml}/`. Los refrescos de referencias Silver usan
`data/manifests/silver/references/` y conservan métricas reales de archivos, filas y duración.

## Rutas principales

```text
data/bronze/                    originales y referencias raw
data/silver/                    datasets curados, rechazados y trips_master
data/gold/                      dimensiones, hechos, marts y features
data/ml/                        predicciones y métricas
data/models/                    modelos Spark persistidos
data/manifests/bronze/          manifests de ingestión
data/manifests/silver/          manifests de curación
data/manifests/gold/            manifests dimensionales
data/manifests/ml/              manifests de entrenamiento
data/audit/                     eventos JSONL e inventario físico
data/tmp/spark/                 spill temporal visible y limitado
```

## Validación del código

```powershell
docker compose run --rm pipeline pytest
```

La entrega fue validada además con compilación Python y análisis estático Ruff. Una prueba real completa con cientos de GB debe ejecutarse en tu equipo porque depende de tus Parquet y del motor Docker local.
