# Guía general de ejecución

## Preparación

```powershell
Copy-Item .env.example .env
docker compose build
docker compose up -d mongodb
```

## Secuencia por capas

```powershell
docker compose run --rm bronze plan
docker compose run --rm bronze historical

docker compose run --rm silver silver-plan
docker compose run --rm silver silver-historical
```

## Secuencia Medallion hasta Silver

```powershell
docker compose run --rm pipeline medallion-historical
```

Para el año incremental:

```powershell
docker compose run --rm pipeline medallion-incremental
```

## Prueba mínima Bronze

```powershell
docker compose run --rm bronze plan `
  --services yellow `
  --start-year 2026 `
  --end-year 2026 `
  --months 1
```

## Prueba mínima Silver

Requiere que el mismo periodo exista en Bronze con estado `READY`:

```powershell
docker compose run --rm silver silver-plan `
  --services yellow `
  --start-year 2026 `
  --end-year 2026 `
  --months 1
```

```powershell
docker compose run --rm silver silver-run `
  --services yellow `
  --start-year 2026 `
  --end-year 2026 `
  --months 1
```

## Documentación específica

- Bronze: `docs/bronze-architecture.md` y `docs/ingestion-strategy.md`.
- Silver: `docs/silver-architecture.md`, `docs/silver-data-quality.md` y `docs/silver-execution-guide.md`.
- Auditoría: `docs/audit-model.md`.
