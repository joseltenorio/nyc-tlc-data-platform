# Calidad de datos Silver

## Principio

Silver no elimina silenciosamente registros problemáticos. Cada fila recibe códigos estructurados y termina en uno de tres estados:

```text
VALID
WARNING
REJECTED
```

Campos de control:

```text
quality_error_codes      array<string>
quality_warning_codes    array<string>
quality_error_count      int
quality_warning_count    int
quality_status           string
```

Los registros `VALID` y `WARNING` se publican en el dataset curado. Los registros `REJECTED` se conservan en `rejected_records` con el mismo lineage técnico.

## Severidades

### ERROR

Invalida la fila para el consumo analítico principal. Ejemplos:

- pickup o dropoff ausente;
- orden temporal inválido;
- pickup fuera del año/mes del archivo;
- duración superior al máximo configurado;
- zona nula o fuera del rango TLC;
- zona no encontrada en Taxi Zone Lookup;
- distancia o total fuera de límites;
- componentes financieros negativos, según configuración;
- identificadores obligatorios de base/licencia ausentes;
- duplicado por `trip_business_id`.

### WARNING

La fila permanece disponible, pero requiere contexto. Ejemplos:

- pasajeros nulos o cero imputados a 1;
- distancia cero cuando no se configuró como rechazo;
- códigos de proveedor, tarifa, pago o banderas inesperados;
- propina o peaje sospechoso;
- base histórica ausente del catálogo vigente;
- diferencia relevante entre `trip_time` informado y duración calculada;
- shared/WAV matched sin solicitud coherente;
- licenciatario HVFHS no reconocido.

## Deduplicación

Silver genera `trip_business_id` mediante SHA-256 sobre un conjunto estable de atributos por servicio. Dentro del archivo procesado:

- la primera ocurrencia permanece;
- las siguientes reciben `DUPLICATE_TRIP` y pasan a rechazados.

No se usa un identificador secuencial dependiente del orden de lectura como clave de negocio.

## Reconciliación

Por cada archivo:

```text
rows_read = rows_valid + rows_rejected
```

Cuando Bronze registró `parquet_num_rows`, también se exige:

```text
bronze_num_rows = rows_read
```

Estados:

```text
MATCHED
MATCHED_WITHOUT_BRONZE_METADATA
```

Cualquier desbalance produce `SilverReconciliationError` y evita marcar el periodo como `READY`.

## Configuración

Los umbrales se encuentran en `config/silver.yml`, entre ellos:

```text
valid_location_id_min / max
taxi_max_duration_hours
fhv_max_duration_hours
max_passenger_count
max_trip_distance_miles
max_total_amount
reject_zero_distance
reject_negative_component_amounts
```

Cambiar un umbral requiere reprocesar los periodos afectados con `--force` y conservar la configuración utilizada como parte de la evidencia de ejecución.

## Auditoría de reglas

`silver_quality_results` almacena una fila documental por:

```text
execution_id + service + year + month + rule_code
```

Incluye severidad y cantidad de filas afectadas. Esto permite construir posteriormente el dashboard de calidad sin guardar viajes individuales en MongoDB.
