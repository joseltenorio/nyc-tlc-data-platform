# Arquitectura Silver

## Propósito

Silver convierte los Parquet originales de Bronze en datasets tipados, homologados y trazables. La capa conserva una salida por servicio, separa los registros rechazados y publica un contrato transversal `trips_master` que será la entrada del futuro modelo dimensional Gold.

Silver no reemplaza ni modifica Bronze. Cada periodo se procesa a partir del archivo vigente registrado como `READY` en `file_registry`.

## Flujo

```text
file_registry Bronze (READY)
            │
            ▼
     SilverSourceCatalog
            │
            ▼
  lectura Parquet con PySpark
            │
            ├── normalización por servicio
            ├── campos derivados y lineage
            ├── trip_business_id
            ├── reglas ERROR/WARNING
            ├── deduplicación
            └── enriquecimiento de referencias
            │
       ┌────┴────┐
       ▼         ▼
   válidos    rechazados
       │         │
       ▼         ▼
Silver servicio rejected_records
       │
       ▼
  trips_master
```

## Componentes

```text
src/tlc_data_platform/silver/
├── transformers/       # transformaciones Yellow, Green, FHV y HVFHV
├── source_catalog.py   # selección de fuentes Bronze READY
├── references.py       # Taxi Zones y Current Bases: Bronze raw + Silver curated
├── enrichment.py       # joins de zonas y bases
├── master.py           # contrato conformado transversal
├── storage.py          # temporales y promoción recuperable
├── audit.py            # agrupación de repositorios Silver
├── manifest.py         # manifiesto por ejecución
├── models.py           # modelos de planificación y resultados
├── spark.py            # SparkSession compartida
└── pipeline.py         # coordinación de la capa
```

## Unidad de procesamiento

La unidad operativa es:

```text
service + year + month + source_sha256
```

Un archivo Bronze no se reprocesa cuando:

- su checksum es igual al registrado en `silver_file_registry`;
- las salidas curada, rechazada y maestra existen;
- no se utilizó `--force`.

## Publicación de salidas

Cada periodo se escribe primero en:

```text
data/tmp/silver/<execution_id>/...
```

Después se promueven coordinadamente las salidas:

```text
curated
rejected
master
```

Antes de sustituir una salida existente, se crea un respaldo temporal `.previous`. Si ocurre un error Python durante la promoción, las rutas publicadas en esa operación se eliminan y se restauran las versiones anteriores. No existe atomicidad de sistema de archivos entre directorios, pero la operación es recuperable frente a fallos controlados del proceso.

## Timestamps

Los timestamps TLC son horas locales de Nueva York sin zona horaria embebida. Silver los representa como:

```text
timestamp_ntz
```

La SparkSession usa:

```text
spark.sql.session.timeZone = America/New_York
```

Esto evita desplazar horas al convertirlas como si fueran instantes UTC.

## Referencias

La capa utiliza:

- Taxi Zone Lookup para pickup y dropoff;
- Current Bases para FHV/HVFHV.

La copia CSV descargada se conserva en Bronze con direccionamiento por contenido:

```text
data/bronze/reference/<dataset>/sha256=<hash>/<file>.csv
```

La versión normalizada se publica en:

```text
data/silver/taxi_zones/
data/silver/base_lookup/
```

`Current Bases` es una fotografía vigente, no un historial completo. Una base histórica ausente genera una advertencia, no el rechazo automático del viaje.

## Preparación para Gold

`trips_master` mantiene únicamente atributos compatibles o útiles entre servicios. Las variables exclusivas permanecen en los datasets Silver por servicio. Esta decisión permite construir después una constelación de hechos sin forzar todos los campos especializados dentro de una sola tabla analítica.
