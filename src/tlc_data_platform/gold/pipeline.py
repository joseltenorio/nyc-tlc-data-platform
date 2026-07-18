from __future__ import annotations

import logging
from dataclasses import asdict
from pathlib import Path
from typing import Any, Iterable
from uuid import uuid4

from tlc_data_platform.audit.gold_dataset_repository import GoldDatasetRepository
from tlc_data_platform.audit.gold_execution_repository import GoldExecutionRepository
from tlc_data_platform.audit.parquet_metrics import aggregate_metrics, parquet_metrics
from tlc_data_platform.audit.processing_attempt_repository import ProcessingAttemptRepository
from tlc_data_platform.audit.unified import UnifiedAuditRepository
from tlc_data_platform.core.settings import AppConfig, RunSelection
from tlc_data_platform.gold.audit import GoldAuditRepositories
from tlc_data_platform.gold.dimensions import (
    build_date_dimension,
    build_payment_type_dimension,
    build_provider_dimension,
    build_rate_code_dimension,
    build_service_dimension,
    build_time_dimension,
    build_trip_type_dimension,
    build_zone_dimension,
)
from tlc_data_platform.gold.facts import (
    build_hvfhv_operations_fact,
    build_taxi_financial_fact,
    build_trip_activity_fact,
)
from tlc_data_platform.gold.features import (
    build_hvfhv_wait_features,
    build_zone_hourly_demand_features,
    build_zone_profile_features,
)
from tlc_data_platform.gold.manifest import GoldManifestWriter
from tlc_data_platform.gold.marts import build_marts
from tlc_data_platform.gold.models import (
    GoldDatasetResult,
    GoldExecutionSummary,
    GoldPlanSummary,
    utc_now,
)
from tlc_data_platform.gold.spark import GoldSparkProvider
from tlc_data_platform.gold.storage import GoldSourcePartition, GoldStorage
from tlc_data_platform.mongodb.client import MongoClientProvider
from tlc_data_platform.mongodb.gold_ml_index_manager import GoldMLIndexManager

LOGGER = logging.getLogger(__name__)

GOLD_STAGES = ("dimensions", "facts", "marts", "features")


class GoldPipeline:
    """Builds Gold sequentially and publishes each output with a recoverable swap.

    The previous implementation cached the complete Silver master plus several
    full facts at once. On a local Docker/WSL host that created hundreds of GB of
    Spark spill. This implementation reads one Silver partition at a time, never
    persists a large frame and builds global marts/features only after facts are
    safely published.
    """

    def __init__(self, config: AppConfig) -> None:
        self._config = config
        self._storage = GoldStorage(config.gold, config.silver.storage, config.services)
        self._spark_provider = GoldSparkProvider(config.gold.spark)
        self._mongo_provider = MongoClientProvider(config.mongo)

    def close(self) -> None:
        self._spark_provider.close()
        self._mongo_provider.close()

    def plan(self, selection: RunSelection) -> GoldPlanSummary:
        partitions, missing = self._storage.selected_master_partitions(selection)
        expected = self._storage.applicable_period_count(selection)
        warnings: list[str] = []
        if not partitions:
            warnings.append("No existen particiones trips_master para la selección.")
        if missing:
            warnings.append(
                "Las particiones Silver ausentes se registrarán como cobertura parcial; nunca como cero."
            )
        return GoldPlanSummary(
            status="READY" if partitions else "NO_INPUT",
            selected_partitions=expected,
            available_partitions=len(partitions),
            missing_partitions=missing,
            source_paths=[str(item.path) for item in partitions],
            warnings=warnings,
        )

    def run(
        self,
        selection: RunSelection,
        *,
        execution_type: str = "run",
        stages: Iterable[str] | None = None,
    ) -> GoldExecutionSummary:
        selected_stages = self._validate_stages(stages)
        self._storage.ensure_directories()
        self._storage.cleanup_stale_temporary()
        execution_id = f"gold-{utc_now().strftime('%Y%m%dT%H%M%S')}-{uuid4().hex[:8]}"
        started_at = utc_now()
        manifest = GoldManifestWriter(self._config.gold.storage.manifests_root, execution_id)
        partitions, missing = self._storage.selected_master_partitions(selection)
        warnings = [f"Partición Silver ausente: {period}" for period in missing]
        audit = self._audit()
        audit.executions.start(execution_id, execution_type, started_at, asdict(selection))
        if audit.unified is not None:
            audit.unified.start_run(
                execution_id,
                layer="gold",
                execution_type=execution_type,
                selection={**asdict(selection), "stages": selected_stages},
                started_at=started_at,
            )

        if not partitions:
            summary = self._summary(
                execution_id,
                execution_type,
                "NO_INPUT",
                started_at,
                selection,
                partitions,
                0,
                [],
                warnings + ["No se encontraron particiones Silver trips_master."],
            )
            audit.executions.finish(summary)
            if audit.unified is not None:
                audit.unified.record_coverage(
                    execution_id,
                    layer="gold",
                    expected_count=self.plan(selection).selected_partitions,
                    available_count=0,
                    ready_count=0,
                    missing=missing,
                )
                audit.unified.finish_run(
                    execution_id,
                    status="NO_INPUT",
                    finished_at=summary.finished_at,
                    metrics={"parquet_partitions_input": 0, "datasets_built": 0},
                )
            manifest.write(summary)
            return summary

        spark = self._spark_provider.get()
        guard = self._spark_provider.guard()
        results: list[GoldDatasetResult] = []
        source_metrics = aggregate_metrics([item.path for item in partitions])
        try:
            if "dimensions" in selected_stages:
                results.extend(
                    self._build_dimensions(spark, guard, audit, execution_id, partitions)
                )

            if "facts" in selected_stages:
                for partition in partitions:
                    results.extend(
                        self._build_fact_partition(
                            spark, guard, audit, execution_id, partition
                        )
                    )
                    spark.catalog.clearCache()

            if "marts" in selected_stages:
                results.extend(self._build_marts(spark, guard, audit, execution_id, partitions))
                spark.catalog.clearCache()

            if "features" in selected_stages:
                results.extend(
                    self._build_features(spark, guard, audit, execution_id, partitions)
                )
                spark.catalog.clearCache()

            status = "PARTIAL_SUCCESS" if missing else "SUCCESS"
            summary = self._summary(
                execution_id,
                execution_type,
                status,
                started_at,
                selection,
                partitions,
                source_metrics.rows or 0,
                results,
                warnings,
            )
            audit.executions.finish(summary)
            self._record_coverage_and_finish(audit, summary, selected_stages, missing)
            manifest.write(summary)
            return summary
        except Exception as error:
            audit.executions.fail(execution_id, utc_now(), error)
            if audit.unified is not None:
                audit.unified.fail_run(execution_id, error, layer="gold")
            LOGGER.exception("Gold falló: %s", error)
            raise
        finally:
            self._storage.cleanup_execution(execution_id)

    def _build_dimensions(
        self,
        spark: Any,
        guard: Any,
        audit: GoldAuditRepositories,
        execution_id: str,
        partitions: list[GoldSourcePartition],
    ) -> list[GoldDatasetResult]:
        taxi_zones = spark.read.parquet(str(self._storage.taxi_zone_path()))
        all_paths = self._storage.scoped_master_paths(
            self._config.period.historical_start_year,
            self._config.period.incremental_year,
        ) or [item.path for item in partitions]
        # Column pruning prevents the provider dimension from materializing the
        # complete wide Silver master.
        provider_columns = [
            "service_type",
            "vendor_id_or_license",
            "dispatching_base_num",
            "dispatching_base_name",
            "dispatching_base_dba",
            "originating_base_num",
            "affiliated_base_num",
            "hvfhs_company_name",
        ]
        provider_source = spark.read.parquet(*[str(path) for path in all_paths]).select(
            *provider_columns
        )
        dimensions = {
            "date": build_date_dimension(
                spark,
                self._config.period.historical_start_year,
                self._config.period.incremental_year,
            ),
            "time": build_time_dimension(spark),
            "service": build_service_dimension(spark),
            "zone": build_zone_dimension(taxi_zones),
            "provider": build_provider_dimension(provider_source),
            "payment_type": build_payment_type_dimension(spark),
            "rate_code": build_rate_code_dimension(spark),
            "trip_type": build_trip_type_dimension(spark),
        }
        results: list[GoldDatasetResult] = []
        for logical_name, frame in dimensions.items():
            results.append(
                self._write_atomic_dataset(
                    guard,
                    audit,
                    execution_id,
                    logical_name,
                    "dimension",
                    frame,
                    self._storage.dimension_path(logical_name),
                )
            )
        return results

    def _build_fact_partition(
        self,
        spark: Any,
        guard: Any,
        audit: GoldAuditRepositories,
        execution_id: str,
        source: GoldSourcePartition,
    ) -> list[GoldDatasetResult]:
        master = spark.read.parquet(str(source.path))
        input_metrics = parquet_metrics(source.path)
        facts: dict[str, Any] = {"trip_activity": build_trip_activity_fact(master)}
        if source.service in {"yellow", "green"}:
            facts["taxi_financial"] = build_taxi_financial_fact(master)
        if source.service == "fhvhv":
            facts["hvfhv_operations"] = build_hvfhv_operations_fact(master)

        results: list[GoldDatasetResult] = []
        for logical_name, frame in facts.items():
            destination = self._storage.fact_partition_path(
                logical_name, source.service, source.year, source.month
            )
            attempt_id = audit.attempts.start(
                execution_id,
                "gold",
                "write_fact_partition",
                f"{logical_name}:{source.period_id}",
                utc_now(),
            )
            try:
                guard.run(
                    lambda frame=frame, logical_name=logical_name: self._storage.write_fact_partition_atomic(
                        frame,
                        logical_name,
                        source.service,
                        source.year,
                        source.month,
                        execution_id,
                    )
                )
                metrics = parquet_metrics(destination)
                result = GoldDatasetResult(
                    dataset_name=self._storage.fact_path(logical_name).name,
                    dataset_type="fact_partition",
                    path=str(destination),
                    status="READY",
                    rows_written=metrics.rows,
                    partition_columns=("service_type", "source_year", "source_month"),
                )
                results.append(result)
                audit.datasets.upsert(execution_id, result)
                audit.attempts.finish(
                    attempt_id,
                    utc_now(),
                    status="SUCCESS",
                    rows_read=input_metrics.rows,
                    rows_written=metrics.rows,
                )
                if audit.unified is not None:
                    audit.unified.record_dataset(
                        execution_id,
                        layer="gold",
                        dataset_name=self._storage.fact_path(logical_name).name,
                        dataset_type="fact_partition",
                        operation="silver_to_gold",
                        status="READY",
                        path=str(destination),
                        parquet_files=metrics.parquet_files,
                        rows=metrics.rows,
                        bytes_on_disk=metrics.bytes_on_disk,
                        service=source.service,
                        year=source.year,
                        month=source.month,
                        source_dataset="silver.trips_master",
                    )
            except Exception as error:
                audit.attempts.finish(attempt_id, utc_now(), status="FAILED", error=error)
                if audit.unified is not None:
                    audit.unified.record_dataset(
                        execution_id,
                        layer="gold",
                        dataset_name=self._storage.fact_path(logical_name).name,
                        dataset_type="fact_partition",
                        operation="silver_to_gold",
                        status="FAILED",
                        service=source.service,
                        year=source.year,
                        month=source.month,
                        source_dataset="silver.trips_master",
                        error=error,
                    )
                raise

        activity_path = self._storage.fact_partition_path(
            "trip_activity", source.service, source.year, source.month
        )
        activity_metrics = parquet_metrics(activity_path)
        source_rows = input_metrics.rows
        target_rows = activity_metrics.rows
        matched = source_rows is None or target_rows == source_rows
        audit.reconciliations.insert_one(
            {
                "execution_id": execution_id,
                "layer": "gold",
                "service": source.service,
                "year": source.year,
                "month": source.month,
                "source_dataset": "silver.trips_master",
                "target_dataset": "gold.fact_trip_activity",
                "source_rows": source_rows,
                "target_rows": target_rows,
                "difference": None if source_rows is None or target_rows is None else target_rows - source_rows,
                "status": "MATCHED" if matched else "FAILED",
                "checked_at": utc_now(),
            }
        )
        if audit.unified is not None:
            audit.unified.record_quality(
                execution_id,
                layer="gold",
                dataset_name="fact_trip_activity",
                rule_code="GOLD_SILVER_ROW_RECONCILIATION",
                dimension="reconciliation",
                severity="ERROR",
                status="PASSED" if matched else "FAILED",
                expected=source_rows,
                actual=target_rows,
                failed_rows=0 if matched else abs((target_rows or 0) - (source_rows or 0)),
                context={"service": source.service, "year": source.year, "month": source.month},
            )
        if not matched:
            raise ValueError(
                f"Reconciliación Silver->Gold falló para {source.period_id}: "
                f"source={source_rows}, target={target_rows}"
            )
        return results

    def _build_marts(
        self,
        spark: Any,
        guard: Any,
        audit: GoldAuditRepositories,
        execution_id: str,
        partitions: list[GoldSourcePartition],
    ) -> list[GoldDatasetResult]:
        activity, taxi, hvfhv, dim_zone = self._analytical_inputs(spark, partitions)
        frames = build_marts(activity, taxi, hvfhv, dim_zone)
        results: list[GoldDatasetResult] = []
        for logical_name, frame in frames.items():
            results.append(
                self._write_atomic_dataset(
                    guard,
                    audit,
                    execution_id,
                    logical_name,
                    "mart",
                    frame,
                    self._storage.mart_path(logical_name),
                )
            )
        return results

    def _build_features(
        self,
        spark: Any,
        guard: Any,
        audit: GoldAuditRepositories,
        execution_id: str,
        partitions: list[GoldSourcePartition],
    ) -> list[GoldDatasetResult]:
        activity, _taxi, hvfhv, dim_zone = self._analytical_inputs(spark, partitions)
        frames = {
            "zone_hourly_demand": build_zone_hourly_demand_features(activity, dim_zone),
            "zone_profiles": build_zone_profile_features(activity, hvfhv, dim_zone),
            "hvfhv_wait_risk": build_hvfhv_wait_features(hvfhv, dim_zone),
        }
        results: list[GoldDatasetResult] = []
        for logical_name, frame in frames.items():
            results.append(
                self._write_atomic_dataset(
                    guard,
                    audit,
                    execution_id,
                    logical_name,
                    "ml_feature",
                    frame,
                    self._storage.feature_path(logical_name),
                )
            )
        return results

    def _analytical_inputs(
        self, spark: Any, partitions: list[GoldSourcePartition]
    ) -> tuple[Any, Any, Any, Any]:
        sample_master = spark.read.parquet(str(partitions[0].path)).limit(0)
        activity_template = build_trip_activity_fact(sample_master)
        taxi_template = build_taxi_financial_fact(sample_master)
        hv_template = build_hvfhv_operations_fact(sample_master)
        activity = self._read_fact_or_empty(spark, "trip_activity", activity_template)
        taxi = self._read_fact_or_empty(spark, "taxi_financial", taxi_template)
        hvfhv = self._read_fact_or_empty(spark, "hvfhv_operations", hv_template)
        dim_zone = spark.read.parquet(str(self._storage.dimension_path("zone")))
        return activity, taxi, hvfhv, dim_zone

    def _write_atomic_dataset(
        self,
        guard: Any,
        audit: GoldAuditRepositories,
        execution_id: str,
        logical_name: str,
        dataset_type: str,
        frame: Any,
        path: Path,
    ) -> GoldDatasetResult:
        physical_name = path.name
        attempt_id = audit.attempts.start(
            execution_id, "gold", f"write_{dataset_type}", physical_name, utc_now()
        )
        try:
            guard.run(
                lambda: self._storage.write_atomic(
                    frame, path, execution_id, f"{dataset_type}-{physical_name}"
                )
            )
            metrics = parquet_metrics(path)
            result = GoldDatasetResult(
                dataset_name=physical_name,
                dataset_type=dataset_type,
                path=str(path),
                status="READY",
                rows_written=metrics.rows,
            )
            audit.datasets.upsert(execution_id, result)
            audit.attempts.finish(
                attempt_id,
                utc_now(),
                status="SUCCESS",
                rows_written=metrics.rows,
            )
            if audit.unified is not None:
                audit.unified.record_dataset(
                    execution_id,
                    layer="gold",
                    dataset_name=physical_name,
                    dataset_type=dataset_type,
                    operation="build_publish",
                    status="READY",
                    path=str(path),
                    parquet_files=metrics.parquet_files,
                    rows=metrics.rows,
                    bytes_on_disk=metrics.bytes_on_disk,
                )
                audit.unified.record_quality(
                    execution_id,
                    layer="gold",
                    dataset_name=physical_name,
                    rule_code="GOLD_PHYSICAL_PARQUET",
                    dimension="completeness",
                    severity="ERROR",
                    status="PASSED" if metrics.parquet_files > 0 else "FAILED",
                    expected="> 0 parquet files",
                    actual=metrics.parquet_files,
                    failed_rows=0 if metrics.parquet_files > 0 else 1,
                )
            return result
        except Exception as error:
            audit.attempts.finish(attempt_id, utc_now(), status="FAILED", error=error)
            if audit.unified is not None:
                audit.unified.record_dataset(
                    execution_id,
                    layer="gold",
                    dataset_name=physical_name,
                    dataset_type=dataset_type,
                    operation="build_publish",
                    status="FAILED",
                    path=str(path),
                    error=error,
                )
            raise

    def _read_fact_or_empty(self, spark: Any, logical_name: str, template: Any) -> Any:
        root = self._storage.fact_path(logical_name)
        paths = self._storage.scoped_fact_paths(
            logical_name,
            self._config.period.historical_start_year,
            self._config.period.incremental_year,
        )
        if paths:
            # basePath restores service/year/month partition columns even though
            # only the explicitly scoped leaf directories are read.
            return (
                spark.read.option("basePath", str(root))
                .parquet(*[str(path) for path in paths])
            )
        return template.limit(0)

    def _record_coverage_and_finish(
        self,
        audit: GoldAuditRepositories,
        summary: GoldExecutionSummary,
        stages: list[str],
        missing_partitions: list[str],
    ) -> None:
        unified = audit.unified
        if unified is None:
            return
        expected_paths: list[Path] = []
        if "dimensions" in stages:
            expected_paths.extend(
                self._storage.dimension_path(name)
                for name in self._config.gold.datasets.dimensions
            )
        if "facts" in stages:
            expected_paths.extend(
                self._storage.fact_path(name) for name in self._config.gold.datasets.facts
            )
        if "marts" in stages:
            expected_paths.extend(
                self._storage.mart_path(name) for name in self._config.gold.datasets.marts
            )
        if "features" in stages:
            expected_paths.extend(
                self._storage.feature_path(name)
                for name in self._config.gold.datasets.ml_features
            )
        absent_outputs = [str(path) for path in expected_paths if not self._storage.has_parquet(path)]
        unified.record_coverage(
            summary.execution_id,
            layer="gold",
            expected_count=len(expected_paths),
            available_count=len(expected_paths) - len(absent_outputs),
            ready_count=len(expected_paths) - len(absent_outputs),
            missing=absent_outputs,
            details=[
                {"path": str(path), "status": "READY" if self._storage.has_parquet(path) else "MISSING"}
                for path in expected_paths
            ],
        )
        unified.record_quality(
            summary.execution_id,
            layer="gold",
            dataset_name="gold_layer",
            rule_code="GOLD_EXPECTED_DATASETS_PRESENT",
            dimension="completeness",
            severity="ERROR",
            status="PASSED" if not absent_outputs else "FAILED",
            expected=len(expected_paths),
            actual=len(expected_paths) - len(absent_outputs),
            failed_rows=len(absent_outputs),
            context={"missing_outputs": absent_outputs, "missing_silver_partitions": missing_partitions},
        )
        unified.finish_run(
            summary.execution_id,
            status=summary.status,
            finished_at=summary.finished_at,
            warnings=summary.warnings,
            metrics={
                "parquet_partitions_input": summary.source_partitions,
                "rows_input": summary.source_rows,
                "datasets_built": summary.datasets_built,
                "datasets_failed": summary.failed_datasets,
                "missing_silver_partitions": len(missing_partitions),
            },
        )

    def _audit(self) -> GoldAuditRepositories:
        database = self._mongo_provider.database()
        GoldMLIndexManager(
            database, self._config.gold.collections, self._config.ml.collections
        ).ensure_indexes()
        names = self._config.gold.collections
        return GoldAuditRepositories(
            executions=GoldExecutionRepository(database[names.pipeline_executions]),
            datasets=GoldDatasetRepository(database[names.dataset_registry]),
            attempts=ProcessingAttemptRepository(database[names.processing_attempts]),
            reconciliations=database[names.reconciliations],
            quality=database[names.quality_results],
            unified=UnifiedAuditRepository(database, self._config.audit),
        )

    @staticmethod
    def _validate_stages(stages: Iterable[str] | None) -> list[str]:
        selected = list(dict.fromkeys(stages or GOLD_STAGES))
        unknown = sorted(set(selected) - set(GOLD_STAGES))
        if unknown:
            raise ValueError(f"Etapas Gold desconocidas: {', '.join(unknown)}")
        return selected

    @staticmethod
    def _summary(
        execution_id: str,
        execution_type: str,
        status: str,
        started_at: Any,
        selection: RunSelection,
        partitions: list[GoldSourcePartition],
        source_rows: int,
        results: list[GoldDatasetResult],
        warnings: list[str],
    ) -> GoldExecutionSummary:
        return GoldExecutionSummary(
            execution_id=execution_id,
            execution_type=execution_type,
            status=status,
            started_at=started_at,
            finished_at=utc_now(),
            selected_start_year=selection.start_year,
            selected_end_year=selection.end_year,
            selected_months=selection.months,
            selected_services=selection.services,
            source_partitions=len(partitions),
            source_rows=source_rows,
            datasets_built=sum(result.status == "READY" for result in results),
            failed_datasets=sum(result.status == "FAILED" for result in results),
            results=results,
            warnings=warnings,
        )
