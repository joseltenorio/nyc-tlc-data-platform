from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import yaml

from tlc_data_platform.core.exceptions import ConfigurationError


# ---------------------------------------------------------------------------
# Configuración compartida por Bronze y Silver
# ---------------------------------------------------------------------------


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
    scope_from: PeriodPoint
    scope_to: PeriodPoint


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
    local_dir: Path
    max_temp_bytes: int
    minimum_free_space_bytes: int


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
class SilverStorageConfig:
    silver_root: Path
    datasets: dict[str, str]
    master_dataset: str
    rejected_dataset: str
    taxi_zones_dataset: str
    base_lookup_dataset: str
    temporary_root: Path
    manifests_root: Path


@dataclass(frozen=True)
class SilverExecutionConfig:
    require_bronze_ready_registry: bool
    continue_on_file_error: bool
    parquet_compression: str
    build_master: bool
    require_reference_data: bool
    refresh_references_if_missing: bool
    refresh_references_before_run: bool
    claim_ttl_minutes: int


@dataclass(frozen=True)
class SilverQualityConfig:
    valid_location_id_min: int
    valid_location_id_max: int
    taxi_max_duration_hours: float
    fhv_max_duration_hours: float
    max_passenger_count: int
    max_trip_distance_miles: float
    max_total_amount: float
    impute_zero_or_null_passenger_count: bool
    reject_zero_distance: bool
    reject_negative_component_amounts: bool
    allowed_store_and_forward_flags: tuple[str, ...]
    allowed_boolean_flags: tuple[str, ...]


@dataclass(frozen=True)
class LayerSparkConfig:
    app_name: str
    master: str
    log_level: str
    driver_memory: str
    shuffle_partitions: int
    local_dir: Path
    max_temp_bytes: int
    minimum_free_space_bytes: int


# Se conserva el nombre para no romper imports de Silver existentes.
SilverSparkConfig = LayerSparkConfig


@dataclass(frozen=True)
class SilverReferenceConfig:
    bronze_root: Path
    taxi_zones_url: str
    base_lookup_url: str
    request_timeout_seconds: int


@dataclass(frozen=True)
class SilverMongoCollections:
    pipeline_executions: str
    file_registry: str
    quality_results: str
    reconciliations: str


@dataclass(frozen=True)
class SilverConfig:
    storage: SilverStorageConfig
    execution: SilverExecutionConfig
    quality: SilverQualityConfig
    spark: LayerSparkConfig
    references: SilverReferenceConfig
    collections: SilverMongoCollections


# ---------------------------------------------------------------------------
# Configuración Gold
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class GoldStorageConfig:
    gold_root: Path
    temporary_root: Path
    manifests_root: Path
    dimensions_root: str
    facts_root: str
    marts_root: str
    features_root: str


@dataclass(frozen=True)
class GoldExecutionConfig:
    parquet_compression: str
    rebuild_dimensions: bool
    rebuild_marts: bool
    rebuild_ml_features: bool
    continue_on_error: bool
    process_partitions_sequentially: bool
    count_rows_before_write: bool
    build_marts_after_facts: bool
    build_ml_features_after_marts: bool


@dataclass(frozen=True)
class GoldDatasetsConfig:
    dimensions: dict[str, str]
    facts: dict[str, str]
    marts: dict[str, str]
    ml_features: dict[str, str]


@dataclass(frozen=True)
class GoldMongoCollections:
    pipeline_executions: str
    dataset_registry: str
    reconciliations: str
    quality_results: str
    processing_attempts: str


@dataclass(frozen=True)
class GoldConfig:
    storage: GoldStorageConfig
    execution: GoldExecutionConfig
    spark: LayerSparkConfig
    datasets: GoldDatasetsConfig
    collections: GoldMongoCollections


# ---------------------------------------------------------------------------
# Configuración Machine Learning
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class MLStorageConfig:
    ml_root: Path
    model_root: Path
    temporary_root: Path
    manifests_root: Path


@dataclass(frozen=True)
class MLExecutionConfig:
    random_seed: int
    parquet_compression: str
    continue_on_model_error: bool


@dataclass(frozen=True)
class ForecastConfig:
    feature_dataset: str
    prediction_dataset: str
    anomaly_dataset: str
    metrics_dataset: str
    train_start: str
    train_end: str
    validation_end: str
    test_end: str
    forecast_horizon_hours: int
    minimum_active_hours: int
    minimum_total_trips: int
    anomaly_zscore_threshold: float
    algorithms: tuple[str, ...]
    gbt_max_iter: int
    gbt_max_depth: int
    random_forest_num_trees: int
    random_forest_max_depth: int


@dataclass(frozen=True)
class SegmentationConfig:
    feature_dataset: str
    assignment_dataset: str
    profile_dataset: str
    metrics_dataset: str
    k_min: int
    k_max: int
    max_iter: int


@dataclass(frozen=True)
class WaitRiskConfig:
    feature_dataset: str
    prediction_dataset: str
    metrics_dataset: str
    importance_dataset: str
    excessive_wait_threshold_seconds: int
    train_end: str
    validation_end: str
    test_end: str
    algorithms: tuple[str, ...]
    logistic_max_iter: int
    random_forest_num_trees: int
    random_forest_max_depth: int
    gbt_max_iter: int
    gbt_max_depth: int


@dataclass(frozen=True)
class MLMongoCollections:
    training_runs: str
    model_registry: str
    prediction_runs: str
    metrics: str
    processing_attempts: str


@dataclass(frozen=True)
class MLConfig:
    storage: MLStorageConfig
    execution: MLExecutionConfig
    spark: LayerSparkConfig
    forecast: ForecastConfig
    segmentation: SegmentationConfig
    wait_risk: WaitRiskConfig
    collections: MLMongoCollections




@dataclass(frozen=True)
class AuditCollectionsConfig:
    pipeline_runs: str
    dataset_events: str
    quality_events: str
    coverage_snapshots: str
    download_attempts: str


@dataclass(frozen=True)
class AuditFilesystemConfig:
    enabled: bool
    root: Path
    pipeline_runs_file: str
    dataset_events_file: str
    quality_events_file: str
    coverage_snapshots_file: str
    download_attempts_file: str
    inventory_snapshots_file: str
    inventory_current_file: str
    layer_roots: dict[str, Path]


@dataclass(frozen=True)
class AuditConfig:
    collections: AuditCollectionsConfig
    filesystem: AuditFilesystemConfig
    max_dashboard_documents: int
    require_physical_parquet: bool
    treat_not_published_as_missing: bool


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
    silver: SilverConfig
    gold: GoldConfig
    ml: MLConfig
    audit: AuditConfig

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


# ---------------------------------------------------------------------------
# Lectura y validación de YAML
# ---------------------------------------------------------------------------


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


def _layer_spark(raw: dict[str, Any], context: str) -> LayerSparkConfig:
    return LayerSparkConfig(
        app_name=str(_require(raw, "app_name", context)),
        master=str(_require(raw, "master", context)),
        log_level=str(_require(raw, "log_level", context)),
        driver_memory=str(_require(raw, "driver_memory", context)),
        shuffle_partitions=int(_require(raw, "shuffle_partitions", context)),
        local_dir=Path(raw.get("local_dir", f"data/tmp/spark/{context.split('.')[0]}")),
        max_temp_bytes=int(raw.get("max_temp_bytes", 32 * 1024**3)),
        minimum_free_space_bytes=int(raw.get("minimum_free_space_bytes", 20 * 1024**3)),
    )


def _string_mapping(raw: dict[str, Any], context: str) -> dict[str, str]:
    if not isinstance(raw, dict):
        raise ConfigurationError(f"{context} debe ser un objeto YAML")
    return {str(key): str(value) for key, value in raw.items()}


def load_config(config_dir: str | Path = "config") -> AppConfig:
    root = Path(config_dir)
    app_raw = _read_yaml(root / "app.yml")
    source_raw = _read_yaml(root / "tlc_sources.yml")
    bronze_raw = _read_yaml(root / "bronze.yml")
    contracts = _read_yaml(root / "schema_contracts.yml")
    silver_raw = _read_yaml(root / "silver.yml")
    gold_raw = _read_yaml(root / "gold.yml")
    ml_raw = _read_yaml(root / "ml.yml")
    audit_raw = _read_yaml(root / "audit.yml")

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

    silver_storage_raw = _require(silver_raw, "storage", "silver.yml")
    silver_datasets_raw = _require(silver_storage_raw, "datasets", "silver.storage")
    silver_execution_raw = _require(silver_raw, "execution", "silver.yml")
    silver_quality_raw = _require(silver_raw, "quality", "silver.yml")
    silver_spark_raw = _require(silver_raw, "spark", "silver.yml")
    silver_references_raw = _require(silver_raw, "references", "silver.yml")
    silver_collections_raw = _require(silver_raw, "mongo_collections", "silver.yml")

    gold_storage_raw = _require(gold_raw, "storage", "gold.yml")
    gold_execution_raw = _require(gold_raw, "execution", "gold.yml")
    gold_datasets_raw = _require(gold_raw, "datasets", "gold.yml")
    gold_collections_raw = _require(gold_raw, "mongo_collections", "gold.yml")

    ml_storage_raw = _require(ml_raw, "storage", "ml.yml")
    ml_execution_raw = _require(ml_raw, "execution", "ml.yml")
    forecast_raw = _require(ml_raw, "forecast", "ml.yml")
    segmentation_raw = _require(ml_raw, "segmentation", "ml.yml")
    wait_risk_raw = _require(ml_raw, "wait_risk", "ml.yml")
    ml_collections_raw = _require(ml_raw, "mongo_collections", "ml.yml")
    audit_collections_raw = _require(audit_raw, "collections", "audit.yml")
    audit_filesystem_raw = _require(audit_raw, "filesystem", "audit.yml")
    audit_layer_roots_raw = _require(
        audit_filesystem_raw, "layer_roots", "audit.filesystem"
    )
    audit_retention_raw = _require(audit_raw, "retention", "audit.yml")
    audit_coverage_raw = _require(audit_raw, "coverage", "audit.yml")

    months = tuple(sorted(set(int(m) for m in _require(period_raw, "months", "period"))))
    if not months or any(month < 1 or month > 12 for month in months):
        raise ConfigurationError("period.months debe contener valores entre 1 y 12")

    historical_start = int(_require(period_raw, "historical_start_year", "period"))
    historical_end = int(_require(period_raw, "historical_end_year", "period"))
    incremental_year = int(_require(period_raw, "incremental_year", "period"))

    services: dict[str, ServiceConfig] = {}
    for name, raw in services_raw.items():
        if name not in contracts:
            raise ConfigurationError(f"No existe contrato de esquema para '{name}'")
        available_from = _period_point(
            _require(raw, "available_from", f"services.{name}"),
            f"services.{name}.available_from",
        )
        scope_from = _period_point(
            raw.get("scope_from", {"year": historical_start, "month": 1}),
            f"services.{name}.scope_from",
        )
        scope_to = _period_point(
            raw.get("scope_to", {"year": incremental_year, "month": 12}),
            f"services.{name}.scope_to",
        )
        if (scope_from.year, scope_from.month) > (scope_to.year, scope_to.month):
            raise ConfigurationError(
                f"services.{name}.scope_from no puede superar scope_to"
            )
        services[name] = ServiceConfig(
            enabled=bool(_require(raw, "enabled", f"services.{name}")),
            file_prefix=str(_require(raw, "file_prefix", f"services.{name}")),
            available_from=available_from,
            scope_from=scope_from,
            scope_to=scope_to,
        )
    if historical_start > historical_end:
        raise ConfigurationError("historical_start_year no puede superar historical_end_year")
    if incremental_year < historical_end:
        raise ConfigurationError("incremental_year no puede ser menor que historical_end_year")

    max_workers = int(_require(download_raw, "max_workers", "download"))
    max_hvfhv_workers = int(_require(download_raw, "max_hvfhv_workers", "download"))
    if max_workers < 1 or max_hvfhv_workers < 1:
        raise ConfigurationError("Los workers deben ser mayores que cero")

    k_min = int(_require(segmentation_raw, "k_min", "ml.segmentation"))
    k_max = int(_require(segmentation_raw, "k_max", "ml.segmentation"))
    if k_min < 2 or k_min > k_max:
        raise ConfigurationError("ml.segmentation requiere 2 <= k_min <= k_max")

    environment_variable = str(_require(project_raw, "environment_variable", "project"))
    environment = os.getenv(
        environment_variable,
        str(_require(project_raw, "default_environment", "project")),
    )

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
            parquet_base_url=str(_require(source_info, "parquet_base_url", "source")).rstrip("/"),
            allowed_hosts=tuple(str(host).lower() for host in _require(source_info, "allowed_hosts", "source")),
        ),
        period=PeriodConfig(historical_start, historical_end, incremental_year, months),
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
            minimum_free_space_bytes=int(_require(storage_raw, "minimum_free_space_bytes", "storage")),
        ),
        download=DownloadConfig(
            parallel_enabled=bool(_require(download_raw, "parallel_enabled", "download")),
            max_workers=max_workers,
            max_hvfhv_workers=max_hvfhv_workers,
            chunk_size_bytes=int(_require(download_raw, "chunk_size_bytes", "download")),
            minimum_file_size_bytes=int(_require(download_raw, "minimum_file_size_bytes", "download")),
            max_retries=int(_require(download_raw, "max_retries", "download")),
            initial_backoff_seconds=float(_require(download_raw, "initial_backoff_seconds", "download")),
            max_backoff_seconds=float(_require(download_raw, "max_backoff_seconds", "download")),
            connect_timeout_seconds=int(_require(download_raw, "connect_timeout_seconds", "download")),
            read_timeout_seconds=int(_require(download_raw, "read_timeout_seconds", "download")),
            calculate_sha256=bool(_require(download_raw, "calculate_sha256", "download")),
            validate_parquet_signature=bool(_require(download_raw, "validate_parquet_signature", "download")),
            continue_on_file_error=bool(_require(download_raw, "continue_on_file_error", "download")),
            claim_ttl_minutes=int(_require(download_raw, "claim_ttl_minutes", "download")),
        ),
        validation=ValidationConfig(
            allow_new_columns=bool(_require(validation_raw, "allow_new_columns", "validation")),
            read_sample_row=bool(_require(validation_raw, "read_sample_row", "validation")),
        ),
        spark=SparkConfig(
            app_name=str(_require(spark_raw, "app_name", "spark")),
            master=str(_require(spark_raw, "master", "spark")),
            log_level=str(_require(spark_raw, "log_level", "spark")),
            driver_memory=str(_require(spark_raw, "driver_memory", "spark")),
            local_dir=Path(spark_raw.get("local_dir", "data/tmp/spark/bronze")),
            max_temp_bytes=int(spark_raw.get("max_temp_bytes", 10 * 1024**3)),
            minimum_free_space_bytes=int(spark_raw.get("minimum_free_space_bytes", 10 * 1024**3)),
        ),
        mongo=MongoConfig(
            uri_environment_variable=str(_require(mongo_raw, "uri_environment_variable", "mongo")),
            default_uri=str(_require(mongo_raw, "default_uri", "mongo")),
            database=str(_require(mongo_raw, "database", "mongo")),
            connect_timeout_ms=int(_require(mongo_raw, "connect_timeout_ms", "mongo")),
            collections=MongoCollections(
                pipeline_executions=str(_require(collections_raw, "pipeline_executions", "collections")),
                file_availability=str(_require(collections_raw, "file_availability", "collections")),
                file_registry=str(_require(collections_raw, "file_registry", "collections")),
                file_versions=str(_require(collections_raw, "file_versions", "collections")),
            ),
        ),
        silver=SilverConfig(
            storage=SilverStorageConfig(
                silver_root=Path(_require(silver_storage_raw, "silver_root", "silver.storage")),
                datasets=_string_mapping(silver_datasets_raw, "silver.storage.datasets"),
                master_dataset=str(_require(silver_storage_raw, "master_dataset", "silver.storage")),
                rejected_dataset=str(_require(silver_storage_raw, "rejected_dataset", "silver.storage")),
                taxi_zones_dataset=str(_require(silver_storage_raw, "taxi_zones_dataset", "silver.storage")),
                base_lookup_dataset=str(_require(silver_storage_raw, "base_lookup_dataset", "silver.storage")),
                temporary_root=Path(_require(silver_storage_raw, "temporary_root", "silver.storage")),
                manifests_root=Path(_require(silver_storage_raw, "manifests_root", "silver.storage")),
            ),
            execution=SilverExecutionConfig(
                require_bronze_ready_registry=bool(_require(silver_execution_raw, "require_bronze_ready_registry", "silver.execution")),
                continue_on_file_error=bool(_require(silver_execution_raw, "continue_on_file_error", "silver.execution")),
                parquet_compression=str(_require(silver_execution_raw, "parquet_compression", "silver.execution")),
                build_master=bool(_require(silver_execution_raw, "build_master", "silver.execution")),
                require_reference_data=bool(_require(silver_execution_raw, "require_reference_data", "silver.execution")),
                refresh_references_if_missing=bool(_require(silver_execution_raw, "refresh_references_if_missing", "silver.execution")),
                refresh_references_before_run=bool(_require(silver_execution_raw, "refresh_references_before_run", "silver.execution")),
                claim_ttl_minutes=int(_require(silver_execution_raw, "claim_ttl_minutes", "silver.execution")),
            ),
            quality=SilverQualityConfig(
                valid_location_id_min=int(_require(silver_quality_raw, "valid_location_id_min", "silver.quality")),
                valid_location_id_max=int(_require(silver_quality_raw, "valid_location_id_max", "silver.quality")),
                taxi_max_duration_hours=float(_require(silver_quality_raw, "taxi_max_duration_hours", "silver.quality")),
                fhv_max_duration_hours=float(_require(silver_quality_raw, "fhv_max_duration_hours", "silver.quality")),
                max_passenger_count=int(_require(silver_quality_raw, "max_passenger_count", "silver.quality")),
                max_trip_distance_miles=float(_require(silver_quality_raw, "max_trip_distance_miles", "silver.quality")),
                max_total_amount=float(_require(silver_quality_raw, "max_total_amount", "silver.quality")),
                impute_zero_or_null_passenger_count=bool(_require(silver_quality_raw, "impute_zero_or_null_passenger_count", "silver.quality")),
                reject_zero_distance=bool(_require(silver_quality_raw, "reject_zero_distance", "silver.quality")),
                reject_negative_component_amounts=bool(_require(silver_quality_raw, "reject_negative_component_amounts", "silver.quality")),
                allowed_store_and_forward_flags=tuple(str(v).upper() for v in _require(silver_quality_raw, "allowed_store_and_forward_flags", "silver.quality")),
                allowed_boolean_flags=tuple(str(v).upper() for v in _require(silver_quality_raw, "allowed_boolean_flags", "silver.quality")),
            ),
            spark=_layer_spark(silver_spark_raw, "silver.spark"),
            references=SilverReferenceConfig(
                bronze_root=Path(_require(silver_references_raw, "bronze_root", "silver.references")),
                taxi_zones_url=str(_require(silver_references_raw, "taxi_zones_url", "silver.references")),
                base_lookup_url=str(_require(silver_references_raw, "base_lookup_url", "silver.references")),
                request_timeout_seconds=int(_require(silver_references_raw, "request_timeout_seconds", "silver.references")),
            ),
            collections=SilverMongoCollections(
                pipeline_executions=str(_require(silver_collections_raw, "pipeline_executions", "silver.mongo_collections")),
                file_registry=str(_require(silver_collections_raw, "file_registry", "silver.mongo_collections")),
                quality_results=str(_require(silver_collections_raw, "quality_results", "silver.mongo_collections")),
                reconciliations=str(_require(silver_collections_raw, "reconciliations", "silver.mongo_collections")),
            ),
        ),
        gold=GoldConfig(
            storage=GoldStorageConfig(
                gold_root=Path(_require(gold_storage_raw, "gold_root", "gold.storage")),
                temporary_root=Path(_require(gold_storage_raw, "temporary_root", "gold.storage")),
                manifests_root=Path(_require(gold_storage_raw, "manifests_root", "gold.storage")),
                dimensions_root=str(_require(gold_storage_raw, "dimensions_root", "gold.storage")),
                facts_root=str(_require(gold_storage_raw, "facts_root", "gold.storage")),
                marts_root=str(_require(gold_storage_raw, "marts_root", "gold.storage")),
                features_root=str(_require(gold_storage_raw, "features_root", "gold.storage")),
            ),
            execution=GoldExecutionConfig(
                parquet_compression=str(_require(gold_execution_raw, "parquet_compression", "gold.execution")),
                rebuild_dimensions=bool(_require(gold_execution_raw, "rebuild_dimensions", "gold.execution")),
                rebuild_marts=bool(_require(gold_execution_raw, "rebuild_marts", "gold.execution")),
                rebuild_ml_features=bool(_require(gold_execution_raw, "rebuild_ml_features", "gold.execution")),
                continue_on_error=bool(_require(gold_execution_raw, "continue_on_error", "gold.execution")),
                process_partitions_sequentially=bool(gold_execution_raw.get("process_partitions_sequentially", True)),
                count_rows_before_write=bool(gold_execution_raw.get("count_rows_before_write", False)),
                build_marts_after_facts=bool(gold_execution_raw.get("build_marts_after_facts", True)),
                build_ml_features_after_marts=bool(gold_execution_raw.get("build_ml_features_after_marts", True)),
            ),
            spark=_layer_spark(_require(gold_raw, "spark", "gold.yml"), "gold.spark"),
            datasets=GoldDatasetsConfig(
                dimensions=_string_mapping(_require(gold_datasets_raw, "dimensions", "gold.datasets"), "gold.datasets.dimensions"),
                facts=_string_mapping(_require(gold_datasets_raw, "facts", "gold.datasets"), "gold.datasets.facts"),
                marts=_string_mapping(_require(gold_datasets_raw, "marts", "gold.datasets"), "gold.datasets.marts"),
                ml_features=_string_mapping(_require(gold_datasets_raw, "ml_features", "gold.datasets"), "gold.datasets.ml_features"),
            ),
            collections=GoldMongoCollections(
                pipeline_executions=str(_require(gold_collections_raw, "pipeline_executions", "gold.mongo_collections")),
                dataset_registry=str(_require(gold_collections_raw, "dataset_registry", "gold.mongo_collections")),
                reconciliations=str(_require(gold_collections_raw, "reconciliations", "gold.mongo_collections")),
                quality_results=str(_require(gold_collections_raw, "quality_results", "gold.mongo_collections")),
                processing_attempts=str(_require(gold_collections_raw, "processing_attempts", "gold.mongo_collections")),
            ),
        ),
        ml=MLConfig(
            storage=MLStorageConfig(
                ml_root=Path(_require(ml_storage_raw, "ml_root", "ml.storage")),
                model_root=Path(_require(ml_storage_raw, "model_root", "ml.storage")),
                temporary_root=Path(_require(ml_storage_raw, "temporary_root", "ml.storage")),
                manifests_root=Path(_require(ml_storage_raw, "manifests_root", "ml.storage")),
            ),
            execution=MLExecutionConfig(
                random_seed=int(_require(ml_execution_raw, "random_seed", "ml.execution")),
                parquet_compression=str(_require(ml_execution_raw, "parquet_compression", "ml.execution")),
                continue_on_model_error=bool(_require(ml_execution_raw, "continue_on_model_error", "ml.execution")),
            ),
            spark=_layer_spark(_require(ml_raw, "spark", "ml.yml"), "ml.spark"),
            forecast=ForecastConfig(
                feature_dataset=str(_require(forecast_raw, "feature_dataset", "ml.forecast")),
                prediction_dataset=str(_require(forecast_raw, "prediction_dataset", "ml.forecast")),
                anomaly_dataset=str(_require(forecast_raw, "anomaly_dataset", "ml.forecast")),
                metrics_dataset=str(_require(forecast_raw, "metrics_dataset", "ml.forecast")),
                train_start=str(_require(forecast_raw, "train_start", "ml.forecast")),
                train_end=str(_require(forecast_raw, "train_end", "ml.forecast")),
                validation_end=str(_require(forecast_raw, "validation_end", "ml.forecast")),
                test_end=str(_require(forecast_raw, "test_end", "ml.forecast")),
                forecast_horizon_hours=int(_require(forecast_raw, "forecast_horizon_hours", "ml.forecast")),
                minimum_active_hours=int(_require(forecast_raw, "minimum_active_hours", "ml.forecast")),
                minimum_total_trips=int(_require(forecast_raw, "minimum_total_trips", "ml.forecast")),
                anomaly_zscore_threshold=float(_require(forecast_raw, "anomaly_zscore_threshold", "ml.forecast")),
                algorithms=tuple(str(v) for v in _require(forecast_raw, "algorithms", "ml.forecast")),
                gbt_max_iter=int(_require(forecast_raw, "gbt_max_iter", "ml.forecast")),
                gbt_max_depth=int(_require(forecast_raw, "gbt_max_depth", "ml.forecast")),
                random_forest_num_trees=int(_require(forecast_raw, "random_forest_num_trees", "ml.forecast")),
                random_forest_max_depth=int(_require(forecast_raw, "random_forest_max_depth", "ml.forecast")),
            ),
            segmentation=SegmentationConfig(
                feature_dataset=str(_require(segmentation_raw, "feature_dataset", "ml.segmentation")),
                assignment_dataset=str(_require(segmentation_raw, "assignment_dataset", "ml.segmentation")),
                profile_dataset=str(_require(segmentation_raw, "profile_dataset", "ml.segmentation")),
                metrics_dataset=str(_require(segmentation_raw, "metrics_dataset", "ml.segmentation")),
                k_min=k_min,
                k_max=k_max,
                max_iter=int(_require(segmentation_raw, "max_iter", "ml.segmentation")),
            ),
            wait_risk=WaitRiskConfig(
                feature_dataset=str(_require(wait_risk_raw, "feature_dataset", "ml.wait_risk")),
                prediction_dataset=str(_require(wait_risk_raw, "prediction_dataset", "ml.wait_risk")),
                metrics_dataset=str(_require(wait_risk_raw, "metrics_dataset", "ml.wait_risk")),
                importance_dataset=str(_require(wait_risk_raw, "importance_dataset", "ml.wait_risk")),
                excessive_wait_threshold_seconds=int(_require(wait_risk_raw, "excessive_wait_threshold_seconds", "ml.wait_risk")),
                train_end=str(_require(wait_risk_raw, "train_end", "ml.wait_risk")),
                validation_end=str(_require(wait_risk_raw, "validation_end", "ml.wait_risk")),
                test_end=str(_require(wait_risk_raw, "test_end", "ml.wait_risk")),
                algorithms=tuple(str(v) for v in _require(wait_risk_raw, "algorithms", "ml.wait_risk")),
                logistic_max_iter=int(_require(wait_risk_raw, "logistic_max_iter", "ml.wait_risk")),
                random_forest_num_trees=int(_require(wait_risk_raw, "random_forest_num_trees", "ml.wait_risk")),
                random_forest_max_depth=int(_require(wait_risk_raw, "random_forest_max_depth", "ml.wait_risk")),
                gbt_max_iter=int(_require(wait_risk_raw, "gbt_max_iter", "ml.wait_risk")),
                gbt_max_depth=int(_require(wait_risk_raw, "gbt_max_depth", "ml.wait_risk")),
            ),
            collections=MLMongoCollections(
                training_runs=str(_require(ml_collections_raw, "training_runs", "ml.mongo_collections")),
                model_registry=str(_require(ml_collections_raw, "model_registry", "ml.mongo_collections")),
                prediction_runs=str(_require(ml_collections_raw, "prediction_runs", "ml.mongo_collections")),
                metrics=str(_require(ml_collections_raw, "metrics", "ml.mongo_collections")),
                processing_attempts=str(_require(ml_collections_raw, "processing_attempts", "ml.mongo_collections")),
            ),
        ),
        audit=AuditConfig(
            collections=AuditCollectionsConfig(
                pipeline_runs=str(_require(audit_collections_raw, "pipeline_runs", "audit.collections")),
                dataset_events=str(_require(audit_collections_raw, "dataset_events", "audit.collections")),
                quality_events=str(_require(audit_collections_raw, "quality_events", "audit.collections")),
                coverage_snapshots=str(_require(audit_collections_raw, "coverage_snapshots", "audit.collections")),
                download_attempts=str(_require(audit_collections_raw, "download_attempts", "audit.collections")),
            ),
            filesystem=AuditFilesystemConfig(
                enabled=bool(_require(audit_filesystem_raw, "enabled", "audit.filesystem")),
                root=Path(_require(audit_filesystem_raw, "root", "audit.filesystem")),
                pipeline_runs_file=str(_require(audit_filesystem_raw, "pipeline_runs_file", "audit.filesystem")),
                dataset_events_file=str(_require(audit_filesystem_raw, "dataset_events_file", "audit.filesystem")),
                quality_events_file=str(_require(audit_filesystem_raw, "quality_events_file", "audit.filesystem")),
                coverage_snapshots_file=str(_require(audit_filesystem_raw, "coverage_snapshots_file", "audit.filesystem")),
                download_attempts_file=str(_require(audit_filesystem_raw, "download_attempts_file", "audit.filesystem")),
                inventory_snapshots_file=str(_require(audit_filesystem_raw, "inventory_snapshots_file", "audit.filesystem")),
                inventory_current_file=str(_require(audit_filesystem_raw, "inventory_current_file", "audit.filesystem")),
                layer_roots={
                    str(layer).lower(): Path(path)
                    for layer, path in audit_layer_roots_raw.items()
                },
            ),
            max_dashboard_documents=int(_require(audit_retention_raw, "max_dashboard_documents", "audit.retention")),
            require_physical_parquet=bool(_require(audit_coverage_raw, "require_physical_parquet", "audit.coverage")),
            treat_not_published_as_missing=bool(_require(audit_coverage_raw, "treat_not_published_as_missing", "audit.coverage")),
        ),
        schema_contracts=contracts,
    )


# ---------------------------------------------------------------------------
# Resolución de rangos de ejecución
# ---------------------------------------------------------------------------


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
        raise ConfigurationError("--max-hvfhv-workers debe estar entre 1 y --workers")

    return RunSelection(
        services=selected_services,
        start_year=selected_start,
        end_year=selected_end,
        months=selected_months,
        workers=selected_workers,
        max_hvfhv_workers=selected_hvfhv_workers,
        continue_on_error=selected_continue,
    )


def resolve_silver_selection(
    config: AppConfig,
    mode: str,
    services: list[str] | None = None,
    start_year: int | None = None,
    end_year: int | None = None,
    months: list[int] | None = None,
    continue_on_error: bool | None = None,
) -> RunSelection:
    base_mode = {
        "silver-historical": "historical",
        "silver-incremental": "incremental",
        "silver-run": "run",
        "silver-plan": "plan",
    }.get(mode, mode)
    return resolve_selection(
        config,
        mode=base_mode,
        services=services,
        start_year=start_year,
        end_year=end_year,
        months=months,
        workers=1,
        max_hvfhv_workers=1,
        continue_on_error=(
            config.silver.execution.continue_on_file_error
            if continue_on_error is None
            else continue_on_error
        ),
    )


def resolve_gold_selection(
    config: AppConfig,
    mode: str,
    services: list[str] | None = None,
    start_year: int | None = None,
    end_year: int | None = None,
    months: list[int] | None = None,
) -> RunSelection:
    base_mode = {
        "gold-historical": "historical",
        "gold-incremental": "incremental",
        "gold-run": "run",
        "gold-plan": "plan",
    }.get(mode, mode)
    return resolve_selection(
        config,
        mode=base_mode,
        services=services,
        start_year=start_year,
        end_year=end_year,
        months=months,
        workers=1,
        max_hvfhv_workers=1,
        continue_on_error=config.gold.execution.continue_on_error,
    )
