# Estrategia de ingesta

1. Se genera la matriz solicitada por servicio, año y mes.
2. Los periodos anteriores a `available_from` quedan `NOT_APPLICABLE`.
3. Se extraen enlaces `.parquet` del HTML oficial.
4. Solo los periodos aplicables faltantes se prueban mediante URL determinista.
5. Los enlaces hallados en el HTML oficial se consideran descubiertos y no se descartan por un probe temporal.
6. El probe remoto solo decide publicación para candidatos `deterministic_fallback`: `404/410 -> NOT_PUBLISHED_YET`, red/timeout -> `FAILED_TO_PROBE`, bloqueos temporales -> `DEFERRED_REMOTE_ACCESS`.
7. Antes de descargar se obtiene metadata remota cuando hace falta, sin degradar enlaces oficiales HTML a `NOT_PUBLISHED_YET`.
8. MongoDB reclama cada periodo para evitar dos procesadores simultáneos y recupera claims vencidos, huérfanos o de ejecuciones ya finalizadas.
9. Los archivos se descargan por bloques a `.part`.
10. Se aplica `fsync`, validación de tamaño, contenido y firma.
11. El checksum y la validación estructural determinan idempotencia y versionado.

Las descargas usan threads porque son operaciones de E/S. Spark no se crea dentro de los workers.