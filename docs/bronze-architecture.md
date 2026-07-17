# Arquitectura Bronze

Bronze es una zona inmutable de aterrizaje. El pipeline conserva cada Parquet oficial tal como fue descargado y mantiene la metadata técnica fuera del archivo.

```text
TLC → discovery → availability matrix → concurrent download → validation → atomic publish
                                                                    ↓
                                                        MongoDB + manifest JSON
```

La estructura del paquete es general (`tlc_data_platform`) para que Silver y Gold puedan añadirse como módulos hermanos más adelante. No se crean esos módulos antes de necesitarlos.

## Separación de responsabilidades

- `core`: configuración, errores, logging y Spark compartible.
- `ingestion`: red, descubrimiento, descarga, checksum y validación.
- `bronze`: modelos, almacenamiento, manifiesto y coordinación de la capa.
- `audit`: repositorios de ejecuciones, disponibilidad, registro vigente y versiones.
- `mongodb`: conexión e índices.
- `orchestration`: punto de entrada reutilizable por CLI o notebooks.
- `cli`: comandos de operación.
