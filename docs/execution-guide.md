# Guía general de ejecución

## 1. Configurar WSL una vez

Cierra Docker Desktop:

```powershell
powershell -ExecutionPolicy Bypass -File .\scripts\configure-wsl.ps1
```

Abre Docker Desktop nuevamente.

## 2. Ejecutar todo

```powershell
powershell -ExecutionPolicy Bypass -File .\scripts\run-all.ps1
```

Dashboard:

```text
http://localhost:8501
```

## Alcance automático

- Yellow, Green y FHV: 2023–2025 y meses publicados de 2026.
- HVFHV: 2023.
- Periodos fuera de alcance: `NOT_APPLICABLE`.
- Meses futuros de 2026: `NOT_PUBLISHED_YET`.

## Modos

```powershell
.\scripts\run-all.ps1 -Mode historical
.\scripts\run-all.ps1 -Mode incremental
.\scripts\run-all.ps1 -Mode all -SkipML
```

## Comandos por capa

```powershell
docker compose run --rm bronze historical
docker compose run --rm silver silver-historical
docker compose run --rm pipeline gold-historical
docker compose run --rm pipeline ml-train
```

Plataforma sin script:

```powershell
docker compose run --rm pipeline platform-historical
docker compose run --rm pipeline platform-incremental --no-train-ml
```

## Seguridad

El comando completo se detiene antes de procesar cuando:

- falta `.wslconfig` o supera los límites permitidos;
- Docker no está disponible;
- el disco no tiene la reserva mínima;
- los temporales Spark superan el máximo configurado;
- una escritura temporal no contiene `_SUCCESS` y Parquet válidos.

## Pruebas

```powershell
docker compose run --rm pipeline pytest
```
