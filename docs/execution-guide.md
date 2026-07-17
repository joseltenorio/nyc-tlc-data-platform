# Guía de ejecución

## Secuencia recomendada

```powershell
docker compose build
docker compose up -d mongodb
docker compose run --rm bronze plan
docker compose run --rm bronze historical
docker compose run --rm bronze incremental
```

## Prueba mínima

```powershell
docker compose run --rm bronze plan --services yellow --start-year 2026 --end-year 2026 --months 1
```

```powershell
docker compose run --rm bronze run --services yellow --start-year 2026 --end-year 2026 --months 1 --dry-run
```

## Reanudación

Una nueva ejecución consulta `file_registry`. Los archivos `READY` con metadata remota sin cambios se omiten. Los reclamos vencen según `claim_ttl_minutes` para permitir recuperación tras una interrupción.
