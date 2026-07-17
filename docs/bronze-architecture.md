# Arquitectura Bronze

Bronze es la zona inmutable de aterrizaje del repositorio Bronze + Silver. Conserva cada Parquet oficial tal como fue descargado y mantiene la metadata técnica fuera del archivo.

```text
TLC → discovery → availability matrix → concurrent download → validation → atomic publish
                                                                    ↓
                                                        MongoDB + manifest JSON
                                                                    ↓
                                                              Silver
```

## Separación de responsabilidades

- `core`: configuración, errores, logging y Spark compartible.
- `ingestion`: red, descubrimiento, descarga, checksum y validación.
- `bronze`: modelos, almacenamiento, manifiesto y coordinación Bronze.
- `silver`: transformación, calidad, referencias y contrato conformado.
- `audit`: repositorios de ejecuciones, disponibilidad, registros y reconciliaciones.
- `mongodb`: conexión e índices.
- `orchestration`: puntos de entrada reutilizables por CLI o notebooks.
- `cli`: comandos Bronze, Silver y Medallion.

Bronze no renombra columnas, no convierte tipos, no elimina filas y no reescribe el Parquet oficial. Esas responsabilidades comienzan en Silver.
