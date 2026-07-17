# Estrategia de ingesta

1. Se genera la matriz solicitada por servicio, año y mes.
2. Los periodos anteriores a `available_from` quedan `NOT_APPLICABLE`.
3. Se extraen enlaces `.parquet` del HTML oficial.
4. Solo los periodos aplicables faltantes se prueban mediante URL determinista.
5. Antes de ejecutar se realiza un probe remoto para tamaño, ETag y fecha de modificación.
6. MongoDB reclama cada periodo para evitar dos procesadores simultáneos.
7. Los archivos se descargan por bloques a `.part`.
8. Se aplica `fsync`, validación de tamaño, contenido y firma.
9. El checksum y la validación estructural determinan idempotencia y versionado.

Las descargas usan threads porque son operaciones de E/S. Spark no se crea dentro de los workers.
