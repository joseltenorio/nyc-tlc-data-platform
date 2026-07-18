from __future__ import annotations

import logging
from dataclasses import replace
from pathlib import Path
from typing import Any
from uuid import uuid4

from tlc_data_platform.audit.ml_model_repository import MLModelRepository
from tlc_data_platform.audit.ml_run_repository import MLRunRepository
from tlc_data_platform.audit.parquet_metrics import parquet_metrics
from tlc_data_platform.audit.processing_attempt_repository import ProcessingAttemptRepository
from tlc_data_platform.audit.unified import UnifiedAuditRepository
from tlc_data_platform.core.settings import AppConfig
from tlc_data_platform.ml.audit import MLAuditRepositories
from tlc_data_platform.ml.forecast import train_forecast
from tlc_data_platform.ml.manifest import MLManifestWriter
from tlc_data_platform.ml.models import MLExecutionSummary, MLPlanSummary, utc_now
from tlc_data_platform.ml.segmentation import train_segmentation
from tlc_data_platform.ml.spark import MLSparkProvider
from tlc_data_platform.ml.storage import MLStorage
from tlc_data_platform.ml.wait_risk import train_wait_risk
from tlc_data_platform.mongodb.client import MongoClientProvider
from tlc_data_platform.mongodb.gold_ml_index_manager import GoldMLIndexManager

LOGGER = logging.getLogger(__name__)
MODEL_NAMES = ("forecast", "segmentation", "wait-risk")


class MLPipeline:
    """Trains one model at a time with bounded Spark spill and atomic outputs."""

    def __init__(self, config: AppConfig) -> None:
        self._config = config
        self._storage = MLStorage(config.ml, config.gold)
        self._spark_provider = MLSparkProvider(config.ml.spark)
        self._mongo_provider = MongoClientProvider(config.mongo)

    def close(self) -> None:
        self._spark_provider.close()
        self._mongo_provider.close()

    def plan(self, models: list[str] | None = None) -> MLPlanSummary:
        requested = self._validate_models(models)
        available = {
            model_name: self._storage.has_parquet(self._storage.feature_path(model_name))
            for model_name in requested
        }
        missing = [
            str(self._storage.feature_path(name))
            for name, exists in available.items()
            if not exists
        ]
        return MLPlanSummary(
            status="READY" if not missing else "BLOCKED",
            requested_models=requested,
            available_features=available,
            missing_paths=missing,
        )

    def run(self, models: list[str] | None = None) -> MLExecutionSummary:
        requested = self._validate_models(models)
        plan = self.plan(requested)
        if plan.missing_paths:
            raise FileNotFoundError(
                "Faltan bases Gold para ML: " + ", ".join(plan.missing_paths)
            )

        self._storage.ensure_directories()
        self._storage.cleanup_stale_temporary()
        run_id = f"ml-{utc_now().strftime('%Y%m%dT%H%M%S')}-{uuid4().hex[:8]}"
        started_at = utc_now()
        manifest = MLManifestWriter(self._config.ml.storage.manifests_root, run_id)
        audit = self._audit()
        audit.runs.start(run_id, started_at, requested)
        if audit.unified is not None:
            audit.unified.start_run(
                run_id,
                layer="ml",
                execution_type="train",
                selection={"models": requested},
                started_at=started_at,
            )
        spark = self._spark_provider.get()
        guard = self._spark_provider.guard()
        results = []
        errors: list[dict[str, str]] = []

        try:
            for model_name in requested:
                attempt_id = audit.attempts.start(
                    run_id, "ml", "train_model", model_name, utc_now()
                )
                try:
                    feature_path = self._storage.feature_path(model_name)
                    feature_metrics = parquet_metrics(feature_path)
                    features = spark.read.parquet(str(feature_path))
                    output = guard.run(
                        lambda model_name=model_name, features=features: self._train_one(
                            model_name, features
                        )
                    )
                    paths = self._publish_outputs(
                        guard, run_id, model_name, output
                    )
                    result = replace(output.result, output_paths=paths)
                    results.append(result)
                    audit.models.register(result)
                    audit.prediction_runs.insert_one(
                        {
                            "run_id": run_id,
                            "model_id": result.model_id,
                            "model_name": result.model_name,
                            "generated_at": result.trained_at,
                            "output_paths": paths,
                            "status": "SUCCESS",
                        }
                    )
                    metric_docs = [
                        {
                            "run_id": run_id,
                            "model_id": result.model_id,
                            "model_name": result.model_name,
                            "algorithm": result.algorithm,
                            "metric_name": name,
                            "metric_value": float(value),
                            "recorded_at": utc_now(),
                        }
                        for name, value in result.metrics.items()
                    ]
                    if metric_docs:
                        audit.metrics.insert_many(metric_docs)
                    audit.attempts.finish(
                        attempt_id,
                        utc_now(),
                        status="SUCCESS",
                        rows_read=feature_metrics.rows,
                    )
                    self._record_model_audit(
                        audit, run_id, model_name, result, paths, feature_metrics
                    )
                except Exception as error:
                    LOGGER.exception("Falló el modelo %s: %s", model_name, error)
                    errors.append(
                        {
                            "model_name": model_name,
                            "error_type": type(error).__name__,
                            "error_message": str(error)[:2000],
                        }
                    )
                    audit.attempts.finish(
                        attempt_id, utc_now(), status="FAILED", error=error
                    )
                    if audit.unified is not None:
                        audit.unified.record_quality(
                            run_id,
                            layer="ml",
                            dataset_name=model_name,
                            rule_code="ML_MODEL_TRAINING",
                            dimension="reliability",
                            severity="ERROR",
                            status="FAILED",
                            message=str(error)[:2000],
                        )
                    if not self._config.ml.execution.continue_on_model_error:
                        raise
                finally:
                    spark.catalog.clearCache()

            status = "SUCCESS" if not errors else (
                "PARTIAL_SUCCESS" if results else "FAILED"
            )
            summary = MLExecutionSummary(
                run_id=run_id,
                status=status,
                started_at=started_at,
                finished_at=utc_now(),
                requested_models=requested,
                trained_models=len(results),
                failed_models=len(errors),
                results=results,
                errors=errors,
            )
            audit.runs.finish(summary)
            self._finish_unified(audit, summary)
            manifest.write(summary)
            return summary
        except Exception as error:
            summary = MLExecutionSummary(
                run_id=run_id,
                status="FAILED",
                started_at=started_at,
                finished_at=utc_now(),
                requested_models=requested,
                trained_models=len(results),
                failed_models=max(1, len(errors)),
                results=results,
                errors=errors
                or [
                    {
                        "model_name": "pipeline",
                        "error_type": type(error).__name__,
                        "error_message": str(error)[:2000],
                    }
                ],
            )
            audit.runs.finish(summary)
            if audit.unified is not None:
                audit.unified.fail_run(run_id, error, layer="ml")
            manifest.write(summary)
            raise
        finally:
            self._storage.cleanup_execution(run_id)

    def _train_one(self, model_name: str, features: Any) -> Any:
        if model_name == "forecast":
            return train_forecast(
                features,
                self._config.ml.forecast,
                seed=self._config.ml.execution.random_seed,
                model_root=self._config.ml.storage.model_root / "forecast",
            )
        if model_name == "segmentation":
            return train_segmentation(
                features,
                self._config.ml.segmentation,
                seed=self._config.ml.execution.random_seed,
                model_root=self._config.ml.storage.model_root / "segmentation",
            )
        return train_wait_risk(
            features,
            self._config.ml.wait_risk,
            seed=self._config.ml.execution.random_seed,
            model_root=self._config.ml.storage.model_root / "wait-risk",
        )

    def _publish_outputs(
        self, guard: Any, run_id: str, model_name: str, output: Any
    ) -> dict[str, str]:
        if model_name == "forecast":
            definitions = {
                "predictions": (
                    output.predictions,
                    self._config.ml.forecast.prediction_dataset,
                ),
                "anomalies": (
                    output.anomalies,
                    self._config.ml.forecast.anomaly_dataset,
                ),
                "metrics": (output.metrics, self._config.ml.forecast.metrics_dataset),
            }
        elif model_name == "segmentation":
            definitions = {
                "assignments": (
                    output.assignments,
                    self._config.ml.segmentation.assignment_dataset,
                ),
                "profiles": (
                    output.profiles,
                    self._config.ml.segmentation.profile_dataset,
                ),
                "metrics": (
                    output.metrics,
                    self._config.ml.segmentation.metrics_dataset,
                ),
            }
        else:
            definitions = {
                "predictions": (
                    output.predictions,
                    self._config.ml.wait_risk.prediction_dataset,
                ),
                "metrics": (output.metrics, self._config.ml.wait_risk.metrics_dataset),
                "feature_importance": (
                    output.feature_importance,
                    self._config.ml.wait_risk.importance_dataset,
                ),
            }

        paths: dict[str, str] = {}
        for role, (frame, dataset_name) in definitions.items():
            path = self._storage.output_path(dataset_name)
            guard.run(
                lambda frame=frame, path=path, role=role: self._storage.write_atomic(
                    frame, path, run_id, f"{model_name}-{role}"
                )
            )
            paths[role] = str(path)
        return paths

    def _record_model_audit(
        self,
        audit: MLAuditRepositories,
        run_id: str,
        model_name: str,
        result: Any,
        paths: dict[str, str],
        feature_metrics: Any,
    ) -> None:
        unified = audit.unified
        if unified is None:
            return
        unified.record_dataset(
            run_id,
            layer="ml",
            dataset_name=self._storage.feature_path(model_name).name,
            dataset_type="feature_input",
            operation="train",
            status="READ",
            path=str(self._storage.feature_path(model_name)),
            parquet_files=feature_metrics.parquet_files,
            rows=feature_metrics.rows,
            bytes_on_disk=feature_metrics.bytes_on_disk,
        )
        for role, path_text in paths.items():
            metrics = parquet_metrics(Path(path_text))
            unified.record_dataset(
                run_id,
                layer="ml",
                dataset_name=Path(path_text).name,
                dataset_type=f"model_{role}",
                operation="publish",
                status="READY",
                path=path_text,
                parquet_files=metrics.parquet_files,
                rows=metrics.rows,
                bytes_on_disk=metrics.bytes_on_disk,
                metadata={
                    "model_name": model_name,
                    "model_id": result.model_id,
                    "algorithm": result.algorithm,
                },
            )
        split_ok = (
            result.training_rows > 0
            and result.validation_rows > 0
            and result.test_rows > 0
        )
        unified.record_quality(
            run_id,
            layer="ml",
            dataset_name=model_name,
            rule_code="ML_TEMPORAL_SPLITS_NON_EMPTY",
            dimension="completeness",
            severity="ERROR",
            status="PASSED" if split_ok else "FAILED",
            expected="training, validation and test > 0",
            actual={
                "training_rows": result.training_rows,
                "validation_rows": result.validation_rows,
                "test_rows": result.test_rows,
            },
            failed_rows=0 if split_ok else 1,
        )
        unified.record_quality(
            run_id,
            layer="ml",
            dataset_name=model_name,
            rule_code="ML_METRICS_RECORDED",
            dimension="observability",
            severity="ERROR",
            status="PASSED" if result.metrics else "FAILED",
            expected="> 0 metrics",
            actual=len(result.metrics),
            failed_rows=0 if result.metrics else 1,
        )

    def _finish_unified(
        self, audit: MLAuditRepositories, summary: MLExecutionSummary
    ) -> None:
        unified = audit.unified
        if unified is None:
            return
        expected = len(summary.requested_models)
        ready = summary.trained_models
        missing = [
            model
            for model in summary.requested_models
            if model not in {result.model_name for result in summary.results}
        ]
        unified.record_coverage(
            summary.run_id,
            layer="ml",
            expected_count=expected,
            available_count=expected,
            ready_count=ready,
            missing=missing,
            details=[
                {
                    "model": model,
                    "status": "READY"
                    if model in {result.model_name for result in summary.results}
                    else "FAILED",
                }
                for model in summary.requested_models
            ],
        )
        unified.record_quality(
            summary.run_id,
            layer="ml",
            dataset_name="ml_layer",
            rule_code="ML_EXPECTED_MODELS_TRAINED",
            dimension="completeness",
            severity="ERROR",
            status="PASSED" if not missing else "FAILED",
            expected=expected,
            actual=ready,
            failed_rows=len(missing),
            context={"missing_models": missing},
        )
        unified.finish_run(
            summary.run_id,
            status=summary.status,
            finished_at=summary.finished_at,
            metrics={
                "models_requested": expected,
                "models_trained": summary.trained_models,
                "models_failed": summary.failed_models,
                "parquet_outputs": sum(
                    len(result.output_paths) for result in summary.results
                ),
            },
        )

    @staticmethod
    def _validate_models(models: list[str] | None) -> list[str]:
        requested = list(dict.fromkeys(models or MODEL_NAMES))
        unknown = sorted(set(requested) - set(MODEL_NAMES))
        if unknown:
            raise ValueError(f"Modelos desconocidos: {', '.join(unknown)}")
        return requested

    def _audit(self) -> MLAuditRepositories:
        database = self._mongo_provider.database()
        GoldMLIndexManager(
            database, self._config.gold.collections, self._config.ml.collections
        ).ensure_indexes()
        names = self._config.ml.collections
        return MLAuditRepositories(
            runs=MLRunRepository(database[names.training_runs]),
            models=MLModelRepository(database[names.model_registry]),
            attempts=ProcessingAttemptRepository(database[names.processing_attempts]),
            prediction_runs=database[names.prediction_runs],
            metrics=database[names.metrics],
            unified=UnifiedAuditRepository(database, self._config.audit),
        )
