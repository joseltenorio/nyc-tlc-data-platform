from __future__ import annotations

from typing import Any

from tlc_data_platform.core.settings import GoldMongoCollections, MLMongoCollections


class GoldMLIndexManager:
    """Creates only the indexes required by Gold/ML audit queries."""

    def __init__(
        self,
        database: Any,
        gold: GoldMongoCollections,
        ml: MLMongoCollections,
    ) -> None:
        self._db = database
        self._gold = gold
        self._ml = ml

    def ensure_indexes(self) -> None:
        self._db[self._gold.pipeline_executions].create_index(
            [("execution_id", 1)], unique=True, name="uq_gold_execution_id"
        )
        self._db[self._gold.pipeline_executions].create_index(
            [("started_at", -1)], name="ix_gold_started_at"
        )
        self._db[self._gold.dataset_registry].create_index(
            [("dataset_name", 1)], unique=True, name="uq_gold_dataset"
        )
        self._db[self._gold.processing_attempts].create_index(
            [("execution_id", 1), ("layer", 1), ("stage", 1)],
            name="ix_processing_attempt_execution_stage",
        )
        self._db[self._ml.training_runs].create_index(
            [("run_id", 1)], unique=True, name="uq_ml_run_id"
        )
        self._db[self._ml.training_runs].create_index(
            [("started_at", -1)], name="ix_ml_run_started_at"
        )
        self._db[self._ml.model_registry].create_index(
            [("model_id", 1)], unique=True, name="uq_ml_model_id"
        )
        self._db[self._ml.model_registry].create_index(
            [("model_name", 1), ("status", 1)], name="ix_ml_model_active"
        )
        self._db[self._ml.metrics].create_index(
            [("run_id", 1), ("model_name", 1), ("metric_name", 1)],
            name="ix_ml_metrics_run_model",
        )
