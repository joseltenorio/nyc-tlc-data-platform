# Arquitectura Bronze

Bronze conserva cada Parquet oficial sin transformar y mantiene metadata técnica fuera del archivo.

```text
alcance esperado → discovery HTML/fallback → disponibilidad → descarga/reintentos
        → validación física y esquema → publicación atómica → MongoDB + manifiesto
```

## Alcance

La matriz se genera con una ventana por servicio. Los periodos fuera del proyecto quedan `NOT_APPLICABLE`; los meses todavía no publicados quedan `NOT_PUBLISHED_YET`.

## Descarga

- un worker y un máximo de un HVFHV simultáneo;
- `.part` nuevo por intento;
- un intento inicial y cinco reintentos;
- `fsync`, tamaño, firma `PAR1`, SHA-256 y validación PySpark;
- publicación atómica e historial de versiones.

Bronze no renombra columnas, convierte tipos, elimina filas ni reescribe el Parquet oficial.
