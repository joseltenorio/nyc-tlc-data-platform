from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import yaml

from tlc_data_platform.core.exceptions import ConfigurationError


@dataclass(frozen=True)
class ProjectConfig:
    name: str
    version: str
    layer: str
    environment: str


@dataclass(frozen=True)
class LoggingConfig:
    level: str
    format: str


@dataclass(frozen=True)
class PeriodPoint:
    year: int
    month: int


@dataclass(frozen=True)
class ServiceConfig:
    enabled: bool
    file_prefix: str
    available_from: PeriodPoint


@dataclass(frozen=True)
class SourceConfig:
    name: str
    publisher: str
    landing_page: str
    parquet_base_url: str
    allowed_hosts: tuple[str, ...]


@dataclass(frozen=True)
class PeriodConfig:
    historical_start_year: int
    historical_end_year: int
    incremental_year: int
    months: tuple[int, ...]


@dataclass(frozen=True)
class DiscoveryConfig:
    strategy: str
    verify_tls: bool
    user_agent: str


@dataclass(frozen=True)
class StorageConfig:
    bronze_root: Path
    versions_root: Path
    temporary_root: Path
    manifests_root: Path
    minimum_free_space_bytes: int


@dataclass(frozen=True)
class DownloadConfig:
    parallel_enabled: bool
    max_workers: int
    max_hvfhv_workers: int
    chunk_size_bytes: int
    minimum_file_size_bytes: int
    max_retries: int
    initial_backoff_seconds: float
    max_backoff_seconds: float
    connect_timeout_seconds: int
    read_timeout_seconds: int
    calculate_sha256: bool
    validate_parquet_signature: bool
    continue_on_file_error: bool
    claim_ttl_minutes: int


@dataclass(frozen=True)
class ValidationConfig:
    allow_new_columns: bool
    read_sample_row: bool


@dataclass(frozen=True)
class SparkConfig:
    app_name: str
    master: str
    log_level: str
    driver_memory: str


@dataclass(frozen=True)
class MongoCollections:
    pipeline_executions: str
    file_availability: str
    file_registry: str
    file_versions: str


@dataclass(frozen=True)
class MongoConfig:
    uri_environment_variable: str
    default_uri: str
    database: str
    connect_timeout_ms: int
    collections: MongoCollections


@dataclass(frozen=True)
class AppConfig:
    project: ProjectConfig
    logging: LoggingConfig
    source: SourceConfig
    period: PeriodConfig
    services: dict[str, ServiceConfig]
    discovery: DiscoveryConfig
    storage: StorageConfig
    download: DownloadConfig
    validation: ValidationConfig
    spark: SparkConfig
    mongo: MongoConfig
    schema_contracts: dict[str, Any]

    def enabled_services(self) -> list[str]:
        return sorted(name for name, cfg in self.services.items() if cfg.enabled)


@dataclass(frozen=True)
class RunSelection:
    services: list[str]
    start_year: int
    end_year: int
    months: list[int]
    workers: int
    max_hvfhv_workers: int
    continue_on_error: bool


def _read_yaml(path: Path) -> dict[str, Any]:
    if not path.is_file():
        raise ConfigurationError(f"No existe el archivo de configuración: {path}")
    with path.open("r", encoding="utf-8") as handle:
        value = yaml.safe_load(handle) or {}
    if not isinstance(value, dict):
        raise ConfigurationError(f"El YAML debe contener un objeto raíz: {path}")
    return value


def _require(mapping: dict[str, Any], key: str, context: str) -> Any:
    if key not in mapping:
        raise ConfigurationError(f"Falta '{key}' en {context}")
    return mapping[key]


def _period_point(raw: dict[str, Any], context: str) -> PeriodPoint:
    return PeriodPoint(
        year=int(_require(raw, "year", context)),
        month=int(_require(raw, "month", context)),
    )


def load_config(config_dir: str | Path = "config") -> AppConfig:
    root = Path(config_dir)
    app_raw = _read_yaml(root / "app.yml")
    source_raw = _read_yaml(root / "tlc_sources.yml")
    bronze_raw = _read_yaml(root / "bronze.yml")
    contracts = _read_yaml(root / "schema_contracts.yml")

    project_raw = _require(app_raw, "project", "app.yml")
    logging_raw = _require(app_raw, "logging", "app.yml")
    source_info = _require(source_raw, "source", "tlc_sources.yml")
    period_raw = _require(source_raw, "period", "tlc_sources.yml")
    services_raw = _require(source_raw, "services", "tlc_sources.yml")
    discovery_raw = _require(source_raw, "discovery", "tlc_sources.yml")
    storage_raw = _require(bronze_raw, "storage", "bronze.yml")
    download_raw = _require(bronze_raw, "download", "bronze.yml")
    validation_raw = _require(bronze_raw, "validation", "bronze.yml")
    spark_raw = _require(bronze_raw, "spark", "bronze.yml")
    mongo_raw = _require(bronze_raw, "mongo", "bronze.yml")
    collections_raw = _require(mongo_raw, "collections", "mongo")

    months = tuple(sorted(set(int(m) for m in _require(period_raw, "months", "period"))))
    if not months or any(month < 1 or month > 12 for month in months):
        raise ConfigurationError("period.months debe contener valores entre 1 y 12")

    services: dict[str, ServiceConfig] = {}
    for name, raw in services_raw.items():
        if name not in contracts:
            raise ConfigurationError(f"No existe contrato de esquema para '{name}'")
        services[name] = ServiceConfig(
            enabled=bool(_require(raw, "enabled", f"services.{name}")),
            file_prefix=str(_require(raw, "file_prefix", f"services.{name}")),
            available_from=_period_point(
                _require(raw, "available_from", f"services.{name}"),
                f"services.{name}.available_from",
            ),
        )

    historical_start = int(_require(period_raw, "historical_start_year", "period"))
    historical_end = int(_require(period_raw, "historical_end_year", "period"))
    incremental_year = int(_require(period_raw, "incremental_year", "period"))
    if historical_start > historical_end:
        raise ConfigurationError("historical_start_year no puede superar historical_end_year")
    if incremental_year < historical_end:
        raise ConfigurationError("incremental_year no puede ser menor que historical_end_year")

    environment_variable = str(
        _require(project_raw, "environment_variable", "project")
    )
    environment = os.getenv(
        environment_variable,
        str(_require(project_raw, "default_environment", "project")),
    )

    max_workers = int(_require(download_raw, "max_workers", "download"))
    max_hvfhv_workers = int(
        _require(download_raw, "max_hvfhv_workers", "download")
    )
    if max_workers < 1 or max_hvfhv_workers < 1:
        raise ConfigurationError("Los workers deben ser mayores que cero")

    return AppConfig(
        project=ProjectConfig(
            name=str(_require(project_raw, "name", "project")),
            version=str(_require(project_raw, "version", "project")),
            layer=str(_require(project_raw, "layer", "project")),
            environment=environment,
        ),
        logging=LoggingConfig(
            level=str(_require(logging_raw, "level", "logging")),
            format=str(_require(logging_raw, "format", "logging")),
        ),
        source=SourceConfig(
            name=str(_require(source_info, "name", "source")),
            publisher=str(_require(source_info, "publisher", "source")),
            landing_page=str(_require(source_info, "landing_page", "source")),
            parquet_base_url=str(
                _require(source_info, "parquet_base_url", "source")
            ).rstrip("/"),
            allowed_hosts=tuple(
                str(host).lower()
                for host in _require(source_info, "allowed_hosts", "source")
            ),
        ),
        period=PeriodConfig(
            historical_start_year=historical_start,
            historical_end_year=historical_end,
            incremental_year=incremental_year,
            months=months,
        ),
        services=services,
        discovery=DiscoveryConfig(
            strategy=str(_require(discovery_raw, "strategy", "discovery")),
            verify_tls=bool(_require(discovery_raw, "verify_tls", "discovery")),
            user_agent=str(_require(discovery_raw, "user_agent", "discovery")),
        ),
        storage=StorageConfig(
            bronze_root=Path(_require(storage_raw, "bronze_root", "storage")),
            versions_root=Path(_require(storage_raw, "versions_root", "storage")),
            temporary_root=Path(_require(storage_raw, "temporary_root", "storage")),
            manifests_root=Path(_require(storage_raw, "manifests_root", "storage")),
            minimum_free_space_bytes=int(
                _require(storage_raw, "minimum_free_space_bytes", "storage")
            ),
        ),
        download=DownloadConfig(
            parallel_enabled=bool(
                _require(download_raw, "parallel_enabled", "download")
            ),
            max_workers=max_workers,
            max_hvfhv_workers=max_hvfhv_workers,
            chunk_size_bytes=int(
                _require(download_raw, "chunk_size_bytes", "download")
            ),
            minimum_file_size_bytes=int(
                _require(download_raw, "minimum_file_size_bytes", "download")
            ),
            max_retries=int(_require(download_raw, "max_retries", "download")),
            initial_backoff_seconds=float(
                _require(download_raw, "initial_backoff_seconds", "download")
            ),
            max_backoff_seconds=float(
                _require(download_raw, "max_backoff_seconds", "download")
            ),
            connect_timeout_seconds=int(
                _require(download_raw, "connect_timeout_seconds", "download")
            ),
            read_timeout_seconds=int(
                _require(download_raw, "read_timeout_seconds", "download")
            ),
            calculate_sha256=bool(
                _require(download_raw, "calculate_sha256", "download")
            ),
            validate_parquet_signature=bool(
                _require(download_raw, "validate_parquet_signature", "download")
            ),
            continue_on_file_error=bool(
                _require(download_raw, "continue_on_file_error", "download")
            ),
            claim_ttl_minutes=int(
                _require(download_raw, "claim_ttl_minutes", "download")
            ),
        ),
        validation=ValidationConfig(
            allow_new_columns=bool(
                _require(validation_raw, "allow_new_columns", "validation")
            ),
            read_sample_row=bool(
                _require(validation_raw, "read_sample_row", "validation")
            ),
        ),
        spark=SparkConfig(
            app_name=str(_require(spark_raw, "app_name", "spark")),
            master=str(_require(spark_raw, "master", "spark")),
            log_level=str(_require(spark_raw, "log_level", "spark")),
            driver_memory=str(_require(spark_raw, "driver_memory", "spark")),
        ),
        mongo=MongoConfig(
            uri_environment_variable=str(
                _require(mongo_raw, "uri_environment_variable", "mongo")
            ),
            default_uri=str(_require(mongo_raw, "default_uri", "mongo")),
            database=str(_require(mongo_raw, "database", "mongo")),
            connect_timeout_ms=int(
                _require(mongo_raw, "connect_timeout_ms", "mongo")
            ),
            collections=MongoCollections(
                pipeline_executions=str(
                    _require(collections_raw, "pipeline_executions", "collections")
                ),
                file_availability=str(
                    _require(collections_raw, "file_availability", "collections")
                ),
                file_registry=str(
                    _require(collections_raw, "file_registry", "collections")
                ),
                file_versions=str(
                    _require(collections_raw, "file_versions", "collections")
                ),
            ),
        ),
        schema_contracts=contracts,
    )


def resolve_selection(
    config: AppConfig,
    mode: str,
    services: list[str] | None = None,
    start_year: int | None = None,
    end_year: int | None = None,
    months: list[int] | None = None,
    workers: int | None = None,
    max_hvfhv_workers: int | None = None,
    continue_on_error: bool | None = None,
) -> RunSelection:
    selected_services = sorted(set(services or config.enabled_services()))
    unknown = sorted(set(selected_services) - set(config.services))
    disabled = sorted(
        service
        for service in selected_services
        if service in config.services and not config.services[service].enabled
    )
    if unknown:
        raise ConfigurationError(f"Servicios desconocidos: {', '.join(unknown)}")
    if disabled:
        raise ConfigurationError(f"Servicios deshabilitados: {', '.join(disabled)}")

    if mode == "historical":
        default_start = config.period.historical_start_year
        default_end = config.period.historical_end_year
    elif mode == "incremental":
        default_start = default_end = config.period.incremental_year
    elif mode in {"run", "plan"}:
        default_start = config.period.historical_start_year
        default_end = config.period.incremental_year
    else:
        raise ConfigurationError(f"Modo no soportado: {mode}")

    selected_start = start_year if start_year is not None else default_start
    selected_end = end_year if end_year is not None else default_end
    selected_months = sorted(set(months or list(config.period.months)))
    selected_workers = workers or config.download.max_workers
    selected_hvfhv_workers = max_hvfhv_workers or config.download.max_hvfhv_workers
    selected_continue = (
        config.download.continue_on_file_error
        if continue_on_error is None
        else continue_on_error
    )

    if selected_start > selected_end:
        raise ConfigurationError("--start-year no puede superar --end-year")
    if not selected_months or any(month < 1 or month > 12 for month in selected_months):
        raise ConfigurationError("--months solo acepta valores entre 1 y 12")
    if selected_workers < 1:
        raise ConfigurationError("--workers debe ser mayor que cero")
    if selected_hvfhv_workers < 1:
        raise ConfigurationError("--max-hvfhv-workers debe ser mayor que cero")
    if "fhvhv" in selected_services and selected_hvfhv_workers > selected_workers:
        raise ConfigurationError(
            "--max-hvfhv-workers debe estar entre 1 y --workers"
        )

    return RunSelection(
        services=selected_services,
        start_year=selected_start,
        end_year=selected_end,
        months=selected_months,
        workers=selected_workers,
        max_hvfhv_workers=selected_hvfhv_workers,
        continue_on_error=selected_continue,
    )
