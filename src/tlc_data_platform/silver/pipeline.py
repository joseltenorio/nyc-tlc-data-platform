from __future__ import annotations

import logging
import uuid
from typing import Any

from tlc_data_platform.audit.silver_execution_repository import SilverExecutionRepository
from tlc_data_platform.audit.silver_file_registry_repository import (
    SilverFileRegistryRepository,
)
from tlc_data_platform.audit.silver_quality_repository import SilverQualityRepository
from tlc_data_platform.audit.silver_reconciliation_repository import (
    SilverReconciliationRepository,
)
from tlc_data_platform.core.exceptions import SilverReconciliationError
from tlc_data_platform.core.settings import AppConfig, RunSelection
from tlc_data_platform.mongodb.client import MongoClientProvider
from tlc_data_platform.mongodb.silver_index_manager import SilverIndexManager
from tlc_data_platform.silver.audit import SilverAuditRepositories
from tlc_data_platform.silver.enrichment import SilverReferenceData, enrich_trip
from tlc_data_platform.silver.manifest import SilverManifestWriter
from tlc_data_platform.silver.master import to_master
from tlc_data_platform.silver.models import (
    SilverExecutionSummary,
    SilverFileOutcome,
    SilverPlanSummary,
    SilverSourceFile,
    SilverTransformContext,
    utc_now,
)
from tlc_data_platform.silver.references import SilverReferencePipeline
from tlc_data_platform.silver.source_catalog import SilverSourceCatalog
from tlc_data_platform.silver.spark import SilverSparkProvider
from tlc_data_platform.silver.storage import SilverStorage
from tlc_data_platform.silver.transformers import get_transformer
from tlc_data_platform.silver.transformers.common import split_valid_rejected

LOGGER = logging.getLogger(__name__)


class SilverPipeline:
    def __init__(
        self,
        config: AppConfig,
        *,
        spark_provider: SilverSparkProvider | None = None,
        mongo_provider: MongoClientProvider | None = None,
        storage: SilverStorage | None = None,
        audit: SilverAuditRepositories | None = None,
        source_catalog: SilverSourceCatalog | None = None,
    ) -> None:
        self._config = config
        self._spark = spark_provider or SilverSparkProvider(config.silver.spark)
        self._mongo = mongo_provider or MongoClientProvider(config.mongo)
        self._storage = storage or SilverStorage(config.silver.storage)
        self._audit = audit
        self._source_catalog = source_catalog

    def close(self) -> None:
        self._spark.close()
        self._mongo.close()

    def _ensure_audit(self) -> tuple[SilverAuditRepositories, Any]:
        database = self._mongo.database()
        if self._audit is None:
            names = self._config.silver.collections
            SilverIndexManager(database, names).ensure_indexes()
            executions = SilverExecutionRepository(database[names.pipeline_executions])
            self._audit = SilverAuditRepositories(
                executions=executions,
                registry=SilverFileRegistryRepository(
                    database[names.file_registry],
                    claim_ttl_minutes=self._config.silver.execution.claim_ttl_minutes,
                    execution_repository=executions,
                ),
                quality=SilverQualityRepository(database[names.quality_results]),
                reconciliations=SilverReconciliationRepository(
                    database[names.reconciliations]
                ),
            )
        return self._audit, database

    def _catalog(self, database: Any) -> SilverSourceCatalog:
        if self._source_catalog is not None:
            return self._source_catalog
        bronze_name = self._config.mongo.collections.file_registry
        return SilverSourceCatalog(self._config, database[bronze_name])

    def plan(self, selection: RunSelection) -> SilverPlanSummary:
        self._storage.ensure_directories()
        warnings: list[str] = []
        database = None
        registry = None
        try:
            audit, database = self._ensure_audit()
            registry = audit.registry
        except Exception as exc:
            warnings.append(f"MongoDB no estuvo disponible: {exc}")
            if self._config.silver.execution.require_bronze_ready_registry:
                expected = (
                    len(selection.services)
                    * (selection.end_year - selection.start_year + 1)
                    * len(selection.months)
                )
                return SilverPlanSummary(
                    services=selection.services,
                    start_year=selection.start_year,
                    end_year=selection.end_year,
                    months=selection.months,
                    expected_periods=expected,
                    bronze_ready_periods=0,
                    bronze_missing_periods=expected,
                    already_processed_periods=0,
                    pending_periods=0,
                    states=[],
                    warnings=warnings,
                )

        catalog = (
            self._catalog(database)
            if database is not None
            else SilverSourceCatalog(self._config, None)
        )
        sources, states = catalog.list(selection)
        already = 0
        if registry is not None:
            for source in sources:
                if registry.is_unchanged(
                    source,
                    self._storage.outputs_exist(
                        source, self._config.silver.execution.build_master
                    ),
                ):
                    already += 1

        if sources and not self._storage.references_exist():
            if self._config.silver.execution.refresh_references_if_missing:
                warnings.append(
                    "Las referencias Silver no existen y se descargarán antes de transformar."
                )
            elif self._config.silver.execution.require_reference_data:
                warnings.append(
                    "Faltan taxi_zones/base_lookup; ejecute 'silver-references' antes de Silver."
                )

        return SilverPlanSummary(
            services=selection.services,
            start_year=selection.start_year,
            end_year=selection.end_year,
            months=selection.months,
            expected_periods=len(states),
            bronze_ready_periods=len(sources),
            bronze_missing_periods=sum(
                state.status == "BRONZE_NOT_READY" for state in states
            ),
            already_processed_periods=already,
            pending_periods=max(0, len(sources) - already),
            states=[state.to_dict() for state in states],
            warnings=warnings,
        )

    def run(
        self,
        selection: RunSelection,
        *,
        execution_type: str,
        force: bool = False,
        refresh_references: bool | None = None,
    ) -> SilverExecutionSummary:
        started_at = utc_now()
        execution_id = (
            f"silver-{started_at.strftime('%Y%m%dT%H%M%S')}-{uuid.uuid4().hex[:8]}"
        )
        self._storage.ensure_directories()
        audit, database = self._ensure_audit()
        manifest = SilverManifestWriter(
            self._config.silver.storage.manifests_root, execution_id
        )
        audit.executions.start(
            execution_id,
            execution_type.upper(),
            started_at,
            {
                "services": selection.services,
                "start_year": selection.start_year,
                "end_year": selection.end_year,
                "months": selection.months,
                "force": force,
                "refresh_references": refresh_references,
            },
        )

        outcomes: list[SilverFileOutcome] = []
        source_file_count = 0
        reference_status: str | None = None
        references: SilverReferenceData | None = None
        try:
            sources, states = self._catalog(database).list(selection)
            source_file_count = len(sources)
            manifest.set_states(states)
            spark = self._spark.get() if sources else None

            if sources and spark is not None:
                reference_status = self._ensure_references(
                    refresh_references=refresh_references
                )
                if self._storage.references_exist():
                    references = SilverReferenceData.load(
                        spark, self._config.silver.storage
                    )
                    references.taxi_zones.persist()
                    references.base_lookup.persist()
                elif self._config.silver.execution.require_reference_data:
                    raise FileNotFoundError(
                        "No existen taxi_zones y base_lookup en Silver. "
                        "Ejecute 'silver-references'."
                    )

            for source in sources:
                outcome = self._process_source(
                    spark,
                    source,
                    execution_id,
                    audit,
                    references=references,
                    force=force,
                )
                outcomes.append(outcome)
                manifest.add(outcome)
                if outcome.status == "FAILED" and not selection.continue_on_error:
                    break

            status = self._execution_status(outcomes, bool(sources))
            summary = self._finish_summary(
                execution_id,
                execution_type,
                status,
                started_at,
                selection,
                outcomes,
                manifest,
                source_file_count=source_file_count,
                reference_refresh_status=reference_status,
            )
            audit.executions.finish(summary)
            return summary
        except Exception as exc:
            LOGGER.exception("La ejecución Silver falló: %s", exc)
            finished = utc_now()
            temporary_summary = self._finish_summary(
                execution_id,
                execution_type,
                "FAILED",
                started_at,
                selection,
                outcomes,
                manifest,
                source_file_count=source_file_count,
                reference_refresh_status=reference_status,
                finished_at=finished,
            )
            audit.executions.fail(
                execution_id, finished, exc, temporary_summary.manifest_path
            )
            raise
        finally:
            if references is not None:
                for frame in (references.taxi_zones, references.base_lookup):
                    try:
                        frame.unpersist()
                    except Exception:
                        pass
            try:
                audit.registry.release_claims_for_execution(execution_id)
            except Exception as exc:
                LOGGER.warning(
                    "No se pudieron liberar todos los claims Silver de %s: %s",
                    execution_id,
                    exc,
                )
            self._storage.cleanup_execution(execution_id)

    def _ensure_references(self, *, refresh_references: bool | None) -> str:
        execution = self._config.silver.execution
        should_refresh = (
            refresh_references
            if refresh_references is not None
            else execution.refresh_references_before_run
        )
        if not self._storage.references_exist() and execution.refresh_references_if_missing:
            should_refresh = True
        if not should_refresh:
            return "REUSED" if self._storage.references_exist() else "MISSING"

        pipeline = SilverReferencePipeline(
            self._config,
            spark_provider=self._spark,
            storage=self._storage,
        )
        try:
            return pipeline.run().status
        finally:
            pipeline.close()

    def _process_source(
        self,
        spark: Any,
        source: SilverSourceFile,
        execution_id: str,
        audit: SilverAuditRepositories,
        *,
        references: SilverReferenceData | None,
        force: bool,
    ) -> SilverFileOutcome:
        include_master = self._config.silver.execution.build_master
        if not force and audit.registry.is_unchanged(
            source, self._storage.outputs_exist(source, include_master)
        ):
            outcome = SilverFileOutcome(source, "SKIPPED_UNCHANGED")
            outcome.finish()
            return outcome
        if not audit.registry.claim(source, execution_id):
            outcome = SilverFileOutcome(source, "SKIPPED_CLAIMED")
            outcome.finish()
            return outcome

        outcome = SilverFileOutcome(source, "PROCESSING")
        transformed = valid = rejected = None
        try:
            raw = spark.read.parquet(str(source.path))
            context = SilverTransformContext(
                service=source.service,
                year=source.year,
                month=source.month,
                source_file=source.path.name,
                source_sha256=source.source_sha256,
                bronze_execution_id=source.bronze_execution_id,
                silver_execution_id=execution_id,
            )
            transformed = get_transformer(source.service)(
                raw, context, self._config.silver.quality
            )
            transformed = enrich_trip(transformed, source.service, references).persist()
            valid, rejected = split_valid_rejected(transformed)
            valid = valid.persist()
            rejected = rejected.persist()

            outcome.rows_read = transformed.count()
            outcome.rows_valid = valid.count()
            outcome.rows_rejected = rejected.count()
            outcome.warning_rows = transformed.filter(
                "quality_warning_count > 0"
            ).count()
            outcome.rule_counts, outcome.rule_severities = self._rule_counts(
                transformed
            )
            outcome.reconciliation_status = self._reconcile(source, outcome)

            writer_options = {
                "compression": self._config.silver.execution.parquet_compression
            }
            curated_temp = self._storage.temp_partition(
                execution_id, source, "curated"
            )
            rejected_temp = self._storage.temp_partition(
                execution_id, source, "rejected"
            )
            master_temp = self._storage.temp_partition(
                execution_id, source, "master"
            )
            for path in (curated_temp, rejected_temp, master_temp):
                if path.exists():
                    import shutil

                    shutil.rmtree(path)
                path.parent.mkdir(parents=True, exist_ok=True)

            valid.write.mode("overwrite").options(**writer_options).parquet(
                str(curated_temp)
            )
            rejected.write.mode("overwrite").options(**writer_options).parquet(
                str(rejected_temp)
            )
            if include_master:
                (
                    to_master(valid, source.service)
                    .write.mode("overwrite")
                    .options(**writer_options)
                    .parquet(str(master_temp))
                )

            curated, rejected_path, master = self._storage.promote(
                execution_id, source, include_master
            )
            outcome.status = "READY"
            outcome.curated_path = str(curated)
            outcome.rejected_path = str(rejected_path)
            outcome.master_path = str(master) if master else None
            outcome.finish()
            audit.quality.replace_for_outcome(outcome, execution_id)
            audit.reconciliations.insert(outcome, execution_id)
            audit.registry.mark_ready(outcome, execution_id)
            return outcome
        except Exception as exc:
            outcome.status = "FAILED"
            outcome.error_type = type(exc).__name__
            outcome.error_message = str(exc)[:1000]
            outcome.finish()
            try:
                audit.registry.mark_failed(outcome, execution_id)
            except Exception as audit_error:
                LOGGER.error(
                    "También falló el registro de error para %s: %s",
                    source.period_id,
                    audit_error,
                )
            LOGGER.exception("Falló Silver para %s: %s", source.period_id, exc)
            return outcome
        finally:
            for frame in (rejected, valid, transformed):
                if frame is not None:
                    try:
                        frame.unpersist()
                    except Exception:
                        pass

    @staticmethod
    def _rule_counts(df: Any) -> tuple[dict[str, int], dict[str, str]]:
        from pyspark.sql import functions as F

        errors = df.select(
            F.explode("quality_error_codes").alias("rule_code"),
            F.lit("ERROR").alias("severity"),
        )
        warnings = df.select(
            F.explode("quality_warning_codes").alias("rule_code"),
            F.lit("WARNING").alias("severity"),
        )
        rows = errors.unionByName(warnings).groupBy("rule_code", "severity").count().collect()
        counts = {row["rule_code"]: int(row["count"]) for row in rows}
        severities = {row["rule_code"]: row["severity"] for row in rows}
        return counts, severities

    @staticmethod
    def _reconcile(source: SilverSourceFile, outcome: SilverFileOutcome) -> str:
        if outcome.rows_read != outcome.rows_valid + outcome.rows_rejected:
            raise SilverReconciliationError(
                f"Desbalance {source.period_id}: read={outcome.rows_read}, "
                f"valid={outcome.rows_valid}, rejected={outcome.rows_rejected}"
            )
        if source.bronze_num_rows is None:
            return "MATCHED_WITHOUT_BRONZE_METADATA"
        if source.bronze_num_rows != outcome.rows_read:
            raise SilverReconciliationError(
                f"Metadata Bronze no coincide para {source.period_id}: "
                f"metadata={source.bronze_num_rows}, Spark={outcome.rows_read}"
            )
        return "MATCHED"

    @staticmethod
    def _execution_status(
        outcomes: list[SilverFileOutcome], had_sources: bool
    ) -> str:
        if not had_sources:
            return "NO_INPUT"
        failed = sum(outcome.status == "FAILED" for outcome in outcomes)
        ready = sum(outcome.status == "READY" for outcome in outcomes)
        if failed and ready:
            return "PARTIAL_SUCCESS"
        if failed:
            return "FAILED"
        return "SUCCESS"

    def _finish_summary(
        self,
        execution_id: str,
        execution_type: str,
        status: str,
        started_at: Any,
        selection: RunSelection,
        outcomes: list[SilverFileOutcome],
        manifest: SilverManifestWriter,
        *,
        source_file_count: int,
        reference_refresh_status: str | None,
        finished_at: Any | None = None,
    ) -> SilverExecutionSummary:
        finished = finished_at or utc_now()
        summary = SilverExecutionSummary(
            execution_id=execution_id,
            execution_type=execution_type,
            status=status,
            started_at=started_at,
            finished_at=finished,
            requested_services=selection.services,
            requested_start_year=selection.start_year,
            requested_end_year=selection.end_year,
            requested_months=selection.months,
            source_files=source_file_count,
            processed_files=sum(o.status == "READY" for o in outcomes),
            skipped_files=sum(o.status.startswith("SKIPPED") for o in outcomes),
            failed_files=sum(o.status == "FAILED" for o in outcomes),
            rows_read=sum(o.rows_read for o in outcomes),
            rows_valid=sum(o.rows_valid for o in outcomes),
            rows_rejected=sum(o.rows_rejected for o in outcomes),
            warning_rows=sum(o.warning_rows for o in outcomes),
            manifest_path=str(manifest.path),
            reference_refresh_status=reference_refresh_status,
        )
        manifest.write(summary)
        return summary
